#!/usr/bin/env python3
"""
One-time backfill of hourly price history for in-scope markets (same scope as the graph:
graph_build.scope — geopolitical or credit-relevant policy), open and settled, into market_snapshots with source='backfill'. Also
records their terms in market_versions. Re-runnable: markets already backfilled are skipped.

  Kalshi:     every series in the graph's Kalshi categories (graph_build.GRAPH_SOURCES) -> events (all statuses) -> hourly candlesticks
              (markets settled before Kalshi's historical cutoff come from /historical/...)
  Polymarket: events under the graph's Polymarket tags (open + closed) -> CLOB /prices-history for the YES token,
              in 14-day windows (the API's max range at hourly fidelity)

Markets with lifetime volume < BACKFILL_MIN_VOLUME are skipped (too thin for event betas).
--leadership restricts recovery to leadership contracts, active or settled within
LEADERSHIP_LOOKBACK_DAYS (default 30), so daily recovery stays bounded.
"""
import argparse, os, sys, time
from functools import partial
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import entities, matching
from collector import (CLOB, KALSHI, get, insert, kalshi_record, load_versions, log, now, num, poly_events,
                       poly_record, run_job, version_rows, when, kalshi_resolution, poly_resolution)
from graph_build import EXCLUDE_REGEX, GRAPH_SOURCES, scope

KALSHI_CATEGORIES = GRAPH_SOURCES["kalshi_core"] | GRAPH_SOURCES["kalshi_scoped"] | GRAPH_SOURCES["kalshi_company"]
POLY_TAGS = sorted(GRAPH_SOURCES["poly_core"] | GRAPH_SOURCES["poly_scoped"] | GRAPH_SOURCES["poly_company"])

MIN_VOLUME = float(os.getenv("BACKFILL_MIN_VOLUME", "1000"))
LEADERSHIP_LOOKBACK_DAYS = int(os.getenv("LEADERSHIP_LOOKBACK_DAYS", "30"))
HOUR = 3600
KALSHI_MAX_CANDLES = 5000   # per request
POLY_WINDOW = 14 * 86400    # max startTs..endTs span at hourly fidelity

def paged(url, key, **params):
    out, cursor = [], None
    while True:
        d = get(url, dict(params, limit=200, cursor=cursor))
        out += d.get(key) or []
        if not (cursor := d.get("cursor")): return out

# ── Kalshi ────────────────────────────────────────────────────────────────────
KALSHI_WORKERS, SERIES_ATTEMPTS = 4, 3   # Kalshi rate-limits (429) a full category sweep at 8 workers

def tolerant(what, key, fetch):
    """fetch() with SERIES_ATTEMPTS tries and a growing pause; on final failure the item is skipped
    and logged (a re-run picks it up) instead of aborting the whole Kalshi discovery."""
    for attempt in range(SERIES_ATTEMPTS):
        try:
            return fetch()
        except Exception as err:
            if attempt == SERIES_ATTEMPTS - 1:
                log.warning("kalshi %s %s skipped: %r", what, key, err); SKIPPED_SERIES.append(key); return []
            time.sleep(30 * (attempt + 1))

def series_events(ticker):
    """Every event (with nested markets) of one Kalshi series."""
    return tolerant("series", ticker, lambda: paged(f"{KALSHI}/events", "events", series_ticker=ticker, with_nested_markets="true"))

SKIPPED_SERIES = []

def kalshi_markets(con, leadership_only=False):
    """(record, is_historical) for every market in KALSHI_CATEGORIES series, any status."""
    series = {s["ticker"]: c for c in KALSHI_CATEGORIES
              for s in get(f"{KALSHI}/series", dict(category=c)).get("series") or []
              if (not leadership_only and c != "Financials") or matching.company_leadership(
                  " ".join([s.get("title") or "", *s.get("tags", [])]))}
    with ThreadPoolExecutor(KALSHI_WORKERS) as ex:
        events = [(e, now()) for es in ex.map(series_events, series) for e in es]
    for e, _ in events:
        if not e.get("category"): e["category"] = series.get(e.get("series_ticker"))
    if SKIPPED_SERIES: log.warning("%d kalshi series skipped after retries", len(SKIPPED_SERIES))
    def rec(e, m, ts):
        return kalshi_record(e, m, ts) | {"resolution": kalshi_resolution(m)}
    yield from ((rec(e, m, ts), False) for e, ts in events for m in e.get("markets") or [])

    # Old settled markets are only served by /historical, one request per event: fetch them only
    # for in-scope events (judged on the event title, before any market is fetched).
    empty = {("kalshi", e["event_ticker"]): (e, ts) for e, ts in events if not e.get("markets")}
    titles = {k: " ".join(filter(None, (e.get("title"), e.get("sub_title")))) for k, (e, _) in empty.items()}
    tags = {k: [e.get("category")] for k, (e, _) in empty.items()}
    full = {k: titles[k] + " " + " ".join(t for t in tags[k] if t) for k in titles}
    keep, _ = entities.discover(con, titles, in_scope=scope(titles, tags, full))
    # Recently settled markets can be absent from nested events but still live on /markets.
    def settled(k):
        regular = tolerant("event", k[1], lambda: paged(f"{KALSHI}/markets", "markets", event_ticker=k[1]))
        if regular: return regular, False
        return tolerant("historical event", k[1], lambda: paged(f"{KALSHI}/historical/markets", "markets", event_ticker=k[1])), True
    with ThreadPoolExecutor(KALSHI_WORKERS) as ex:
        hist = ex.map(settled, keep)
        for k, (ms, historical) in zip(keep, hist):
            e, ts = empty[k]
            yield from ((rec(e, m, ts), historical) for m in ms)

def dollars(d, k):  # live candles use "<k>_dollars", historical ones plain "<k>"
    d = d or {}
    return num(d.get(f"{k}_dollars", d.get(k)))

def kalshi_history(rec, hist):
    c, mid = rec["contract"], rec["market_id"]
    start = when(c["open_time"])
    if not start: return []
    end = int(min(when(c["close_time"]) or now(), now()).timestamp())
    url = (f"{KALSHI}/historical/markets/{mid}/candlesticks" if hist
           else f"{KALSHI}/series/{c['series_id']}/markets/{mid}/candlesticks")
    candles, t0 = {}, int(start.timestamp())
    while t0 < end:
        t1 = min(t0 + (KALSHI_MAX_CANDLES - 1) * HOUR, end)
        for x in get(url, dict(start_ts=t0, end_ts=t1, period_interval=60)).get("candlesticks") or []:
            candles[x["end_period_ts"]] = x
        t0 = t1
    rows, window, vol = [], deque(), 0.0
    for t in sorted(candles):
        x = candles[t]
        v = num(x.get("volume_fp", x.get("volume"))) or 0.0
        window.append((t, v)); vol += v
        while window[0][0] <= t - 24 * HOUR:
            vol -= window.popleft()[1]
        price = x.get("price") or {}
        last = dollars(price, "close")
        rows.append(dict(ts=datetime.fromtimestamp(t, timezone.utc),
                         yes_bid=dollars(x.get("yes_bid"), "close"),
                         yes_ask=dollars(x.get("yes_ask"), "close"),
                         last=last if last is not None else dollars(price, "previous"),
                         volume_24h=vol,
                         open_interest=num(x.get("open_interest_fp", x.get("open_interest")))))
    return rows

# ── Polymarket ────────────────────────────────────────────────────────────────
def poly_markets(con, leadership_only=False):
    seen = set()
    # Recover already collected leadership events even when the venue omits its CEO tag.
    if leadership_only:
        rows = con.execute("""SELECT event_id, event_title, title, subtitle, tags::VARCHAR FROM v_market_latest
                              WHERE venue = 'polymarket' AND regexp_matches(lower(concat_ws(' ', event_title, title, subtitle, tags::VARCHAR)),
                              'ceo|cfo|coo|cto|chief|board|executive|rejoin|reinstat')""").fetchall()
        ids = sorted({str(e) for e, *parts in rows if matching.company_leadership(" ".join(str(p or "") for p in parts))})
        for eid in ids:
            e = get(f"https://gamma-api.polymarket.com/events/{eid}")
            for m in e.get("markets") or []:
                if m["id"] not in seen:
                    seen.add(m["id"])
                    yield poly_record(e, m, now()) | {"resolution": poly_resolution(m)}, False
    for tag in (["ceo"] if leadership_only else POLY_TAGS):
        for closed in ("false", "true"):
            for e, ts in poly_events(tag, closed):
                for m in e.get("markets") or []:
                    if m["id"] not in seen:
                        seen.add(m["id"])
                        yield poly_record(e, m, ts) | {"resolution": poly_resolution(m)}, False

def poly_history(rec, _):
    c = rec["contract"]
    start, tokens = when(c["open_time"]), c["clob_token_ids"] or []
    if not start or not tokens: return []
    end = now()
    if rec["status"] != "active" and (close := when(c["close_time"])):
        end = min(end, close + timedelta(days=2))  # resolution usually lands shortly after endDate
    points, t0, end = {}, int(start.timestamp()), int(end.timestamp())
    while t0 < end:
        t1 = min(t0 + POLY_WINDOW, end)
        for x in get(f"{CLOB}/prices-history", dict(market=tokens[0], startTs=t0, endTs=t1,
                                                     fidelity=60)).get("history") or []:
            points[x["t"]] = num(x["p"])
        t0 = t1
    return [dict(ts=datetime.fromtimestamp(t, timezone.utc), yes_bid=None, yes_ask=None, last=p,
                 volume_24h=None, open_interest=None) for t, p in sorted(points.items())]

# ── run ───────────────────────────────────────────────────────────────────────
VENUES = (("kalshi", kalshi_markets, kalshi_history), ("polymarket", poly_markets, poly_history))

def backfill(con, run_id, errors, stats, leadership_only=False):
    stats |= dict(leadership_only=leadership_only, leadership_lookback_days=LEADERSHIP_LOOKBACK_DAYS if leadership_only else None)
    done = set(con.execute("SELECT DISTINCT venue, market_id FROM market_snapshots "
                           "WHERE source = 'backfill'").fetchall())
    versions, todo = load_versions(con), []
    resolved = set(con.execute("SELECT venue, market_id FROM market_resolutions").fetchall())
    for venue, discover, history in VENUES:
        try:  # liquid, not yet backfilled, then in scope (entity lookups only for these)
            cands = [(r, h) for r, h in discover(con, leadership_only=leadership_only) if (r["volume"] or 0) >= MIN_VOLUME
                     and (venue, r["market_id"]) not in done and not EXCLUDE_REGEX.search(r["text"])]
            if leadership_only:
                cutoff = now() - timedelta(days=LEADERSHIP_LOOKBACK_DAYS)
                def recent(r):
                    resolved_at = (r.get("resolution") or {}).get("resolved_at")
                    ended = resolved_at or when(r["contract"].get("close_time"))
                    return r["status"] == "active" or not ended or ended >= cutoff
                cands = [(r, h) for r, h in cands if matching.company_leadership(r["text"]) and recent(r)]
            questions = {(r["venue"], r["market_id"]): matching.question_text(r["contract"]) for r, _ in cands}
            tags = {(r["venue"], r["market_id"]): r["contract"]["tags"] or [] for r, _ in cands}
            full = {(r["venue"], r["market_id"]): r["text"] for r, _ in cands}
            keep, _ = entities.discover(con, questions, in_scope=scope(questions, tags, full))
            scoped = [(r, h) for r, h in cands if (r["venue"], r["market_id"]) in keep]
        except Exception as err:
            log.exception("%s discovery failed", venue); errors.append(f"{venue} discovery: {err!r}")
            continue
        vers = version_rows([r for r, _ in scoped], versions, run_id)
        resolutions = [dict(r["resolution"], observed_at=now(), run_id=run_id) for r, _ in scoped
                       if r.get("resolution") and (r["venue"], r["market_id"]) not in resolved]
        con.begin(); insert(con, "market_versions", vers); insert(con, "market_resolutions", resolutions); con.commit()
        todo += [(r, h, history) for r, h in scoped]
        stats[venue] = dict(candidates=len(cands), in_scope=len(scoped), new_versions=len(vers), resolutions=len(resolutions))
        log.info("%s: %s", venue, stats[venue])

    def one(item):
        r, hist, history = item
        try: return r, history(r, hist), None
        except Exception as err: return r, [], f"{r['venue']} {r['market_id']}: {err!r}"

    batch, n_rows, n_failed = [], 0, 0
    with ThreadPoolExecutor(8) as ex:
        for i, (r, rows, err) in enumerate(ex.map(one, todo), 1):
            if err:
                n_failed += 1
                if n_failed <= 20: errors.append(err)
                log.warning(err)
            batch += [dict(source="backfill", run_id=run_id, venue=r["venue"], market_id=r["market_id"],
                           status=None, **x) for x in rows]
            if len(batch) > 50_000 or i == len(todo):  # commit as we go so a crash keeps progress
                con.begin(); insert(con, "market_snapshots", batch); con.commit()
                n_rows += len(batch); batch = []
                log.info("%d/%d markets, %d rows", i, len(todo), n_rows)
    stats |= {"rows": n_rows, "failed_markets": n_failed, "kalshi_series_skipped": len(SKIPPED_SERIES)}
    return n_failed < len(todo) or not todo

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--leadership", action="store_true", help="Recover only company leadership history, including settled contracts")
    args = parser.parse_args()
    sys.exit(run_job("backfill", partial(backfill, leadership_only=args.leadership)))
