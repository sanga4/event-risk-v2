"""Leadership coverage from collection to issuer links, with no assumed stock-return sign."""
import json
from collections import defaultdict
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

import api
import backfill
import collector
import entities
import graph_build as gb
import matching


@pytest.mark.parametrize("name", ["NVIDIA", "Nvidia", "nvidia", "nViDiA"])
def test_issuer_aliases_ignore_case(name):
    assert gb.company_issuers(f"Will Jensen Huang leave as {name} CEO?") == ["Q182477"]


def test_financials_leadership_is_collected_but_price_ranges_stay_excluded():
    assert collector.kalshi_keep(dict(category="Financials", title="When will Jensen Huang leave as Nvidia CEO?"))
    assert not collector.kalshi_keep(dict(category="Financials", title="Will NVIDIA close above $200?"))
    assert not collector.kalshi_keep(dict(category="Sports", title="Will a team executive resign?"))
    assert collector.poly_keep(dict(title="Jane announced resignation by December?", tags=[{"slug": "finance"}, {"slug": "ceo"}]))


@pytest.mark.parametrize("venue,tags,title", [
    ("kalshi", ["Financials"], "Will Jensen Huang leave as Nvidia CEO?"),
    ("polymarket", ["business", "ceo", "nvidia"], "Jensen Huang announced resignation or removal by December?"),
    ("polymarket", ["business", "tech"], "Will Jensen Huang rejoin NVIDIA?"),
])
def test_leadership_is_a_company_source_and_in_scope(venue, tags, title):
    text = title + " " + " ".join(tags)
    m = dict(venue=venue, title=title, tags=json.dumps(tags))
    assert gb.source_class(m) == "company"
    key = (venue, "test")
    assert gb.scope({key: title}, {key: tags}, {key: text})(key, set(), {})


def test_company_tags_do_not_admit_price_ranges_or_personal_trivia():
    for title in ("Will NVIDIA close above $200?", "Jensen Huang divorce by December?", "Will NVIDIA be mentioned on TV?"):
        assert gb.source_class(dict(venue="polymarket", title=title, tags='["business", "tech", "nvidia"]')) is None
    assert gb.source_class(dict(venue="kalshi", title="Will NVIDIA close above $200?", tags='["Financials"]')) is None


@pytest.mark.parametrize("title,action", [
    ("Jensen Huang announced resignation or removal by December 31, 2026?", "leadership_announcement"),
    ("Will Jensen Huang leave as Nvidia CEO before Jan 1, 2027?", "leadership_departure"),
    ("Will Jensen Huang rejoin NVIDIA by December 31, 2026?", "leadership_return"),
    ("Will Jensen Huang remain Nvidia CEO by December 31, 2026?", "leadership_tenure"),
    ("Will Colette Kress be the next CEO of NVIDIA?", "leadership_appointment"),
    ("Will Jensen Huang leave the Nvidia board before Jan 1, 2027?", "leadership_departure"),
])
def test_leadership_actions_have_relevance_without_an_assumed_sign(title, action):
    m = dict(venue="polymarket", market_id="test", event_id="e", title=title, company=True,
             rules_primary="This market resolves Yes if the named NVIDIA CEO satisfies the condition.")
    sig = matching.parse(m, [])
    assert sig.action == action
    channel, weight, sign = matching.company_effect(sig, title)
    assert (channel, weight, sign) == ("governance", 0.8, None)


def leadership_market():
    t = datetime(2026, 11, 8, tzinfo=timezone.utc)
    title = "Jensen Huang announced resignation or removal by December 31, 2026?"
    return dict(venue="polymarket", market_id="test", event_id="e", series_id=None, event_title=title,
                title=title, subtitle=None, text=title + " ceo nvidia business", company=True,
                rules_primary="This market resolves Yes if NVIDIA's CEO announces resignation by December 31, 2026.",
                open_time=t, close_time=datetime(2027, 1, 1, tzinfo=timezone.utc), first_seen_at=t,
                listed_at=t, priced_at=t, neg_risk=False, strike_type=None, floor_strike=None, cap_strike=None)


def test_build_adds_direct_nvidia_issuer_and_security_path_without_wikipedia():
    m = leadership_market()
    ents = entities.EntityGraph({("polymarket", "test"): []}, {}, {}, [])
    nodes, edges = gb.build([m], lambda requests: {}, ents)
    about = [e for e in edges.values() if e["edge_type"] == "about" and e["dst_id"] == "iss:Q182477"]
    assert len(about) == 1
    assert about[0]["attrs"]["channel"] == "governance" and about[0]["sign"] is None
    assert "sec:NVDA" in nodes


@pytest.mark.parametrize("series_tags", [None, []])
def test_recently_settled_kalshi_markets_use_regular_endpoint_before_archive(series_tags):
    event = dict(event_ticker="KXCEONVDA-E", series_ticker="KXCEONVDA", category="Financials",
                 title="When will Jensen Huang leave as Nvidia CEO?", markets=[])
    market = dict(ticker="KXCEONVDA-E-A", title=event["title"], status="finalized", result="yes", volume_fp="10000")
    def get(url, params=None):
        if url.endswith("/series"):
            return {"series": [dict(ticker="KXCEONVDA", title="Nvidia CEO leaving", tags=series_tags)] if params["category"] == "Financials" else []}
        raise AssertionError(url)
    def paged(url, key, **params):
        assert url.endswith("/markets") and "/historical/" not in url
        return [market]
    with patch.object(backfill, "get", side_effect=get), patch.object(backfill, "series_events", return_value=[event]), \
         patch.object(backfill, "paged", side_effect=paged), \
         patch.object(backfill.entities, "discover", side_effect=lambda con, texts, **kw: (dict.fromkeys(texts, []), {})):
        rows = list(backfill.kalshi_markets(None, leadership_only=True))
    assert len(rows) == 1 and rows[0][1] is False
    assert rows[0][0]["resolution"]["settlement_value"] == 1.0


def test_direct_leadership_listing_includes_unmeasured_contract():
    q, iid = "q:leadership", "iss:Q182477"
    g = type("G", (), {})()
    g.nodes = {q: dict(id=q, type="question", name="Will the NVIDIA CEO leave?", attrs={})}
    g.inc = {iid: [dict(src=q, type="about", attrs={"channel": "governance"})]}
    g.sec_betas = {}
    qp = {q: dict(prob=0.4, status="live", headline=None, volume_24h=100)}
    with patch.object(api, "with_changes", return_value=(qp, {q: {}})):
        rows = api.issuer_leadership_rows(None, g, iid, "NVDA", datetime.now(timezone.utc), [])
    assert rows[0]["id"] == q and rows[0]["move"] is None
    assert "insufficient" in rows[0]["link_note"]


@pytest.mark.parametrize("leadership_only,expected", [(True, False), (False, True)])
def test_leadership_recovery_fails_when_a_venue_cannot_be_discovered(leadership_only, expected):
    def broken(con, **kwargs):
        raise RuntimeError("venue discovery failed")
    con = MagicMock()
    con.execute.return_value.fetchall.return_value = []
    stats, errors = {}, []
    venues = (("kalshi", broken, None), ("polymarket", lambda con, **kw: [], None))
    with patch.object(backfill, "VENUES", venues), patch.object(backfill, "load_versions", return_value={}), \
         patch.object(backfill.entities, "discover", return_value=({}, {})):
        result = backfill.backfill(con, "test", errors, stats, leadership_only=leadership_only)
    assert result is expected
    assert stats["discovery_failures"] == ["kalshi"] and errors


def test_direct_and_issuer_as_entity_paths_do_not_double_count():
    g = type("G", (), {})()
    g.inc = defaultdict(list, {
        "iss:Q182477": [dict(src="q:1", type="about", weight=0.8, sign=None, attrs={"channel": "governance"}),
                         dict(src="wd:Q182477", type="exposure", weight=1, sign=1, attrs={})],
        "wd:Q182477": [dict(src="q:1", type="involves", weight=0.8, sign=None, attrs={})]})
    g.linked = lambda ent: []
    result = api.Graph.exposures_of(g, "iss:Q182477")["q:1"]
    assert result["score"] == 0.8 and len(result["paths"]) == 1
