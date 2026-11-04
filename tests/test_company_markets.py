"""Company markets (earnings, KPIs): issuer matching, the company vocabulary (company sources only),
signs, scope and the read side (exposures, sector links). No database.

  python -m pytest tests
"""
import sys
from collections import defaultdict
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import factors, graph_build as gb, matching

NAMES = {}
def names(ids):
    if not NAMES:
        import csv
        NAMES.update({r["issuer_id"]: r["name"] for r in csv.DictReader(open(gb.ROOT / "issuers.csv"))})
    return [NAMES[i] for i in ids]

@pytest.mark.parametrize("text, expected", [
    ("KXTSLAA Tesla total deliveries in 2026", ["Tesla"]),                       # Kalshi KPI series ticker
    ("KXPMA Zyn US shipment volume (pouches) in fiscal 2026", ["Philip Morris International"]),
    ("Will Apple (AAPL) Q4 Greater China revenue be above $20B?", ["Apple"]),     # ticker in parentheses
    ("Meta advertisement impressions growth in fiscal 2026", ["Meta Platforms"]),  # name without its suffix
    ("Home Depot customer transactions in fiscal 2026", ["Home Depot (The)"]),
    ("Will Roku (ROKU) beat quarterly earnings?", []),                           # not an issuer
    ("Will home sales rise in Taiwan?", []),                                     # generic words never match
])
def test_company_issuers(text, expected):
    assert names(gb.company_issuers(text)) == expected

def market(title, company=True):
    return dict(venue="polymarket", market_id="x", event_id="e", title=title, subtitle=None, rules_primary="", company=company)

@pytest.mark.parametrize("title, action, sign", [
    ("Will Roku (ROKU) beat quarterly earnings?", "earnings_beat", 1.0),
    ("Will Tesla deliver more than 1.8M vehicles in 2026?", "company_metric", 1.0),
    ("Will Tesla deliver fewer than 1.5M vehicles in 2026?", "company_metric", -1.0),
    ("Will General Mills Q2 organic net sales growth be below -4.5%?", "company_metric", -1.0),
    ("Yum! Brands (YUM) Q3 worldwide same-restaurant sales growth <-0.5%?", "company_metric", -1.0),
    ("CVS Health (CVS) Q3 medical benefit ratio (MBR) 93.5%+?", "company_metric", -1.0),   # a cost ratio: more is worse
    ("Walmart (WMT) Q3 US comp sales (ex-fuel) growth 1% to 2.5%?", "company_metric", None),  # a range: no direction
    ("Will Tesla report more than 120k total employee headcount?", "company_metric", None),
])
def test_company_actions_and_signs(title, action, sign):
    sig = matching.parse(market(title), [])
    assert sig.action == action
    assert matching.company_effect(sig, title)[2] == sign

def test_company_vocabulary_only_for_company_sources():
    q = "Will tariff revenue be above $30B in October?"
    assert matching.parse(market(q, company=False), []).action == "tariff_change"
    assert matching.parse(market(q, company=True), []).action == "company_metric"
    assert matching.parse(dict(market("Will Russia invade Moldova?"), company=False), []).action == "invade"

def test_scope_company_sources():
    texts = {("kalshi", "a"): "KXTSLAA Tesla total deliveries in 2026", ("kalshi", "b"): "KXFYRE Fyre Festival ticket price",
             ("polymarket", "c"): "Will Roku (ROKU) beat quarterly earnings?", ("kalshi", "d"): "Senate confirms the nominee"}
    tags = {("kalshi", "a"): ["Companies"], ("kalshi", "b"): ["Companies"], ("polymarket", "c"): ["earnings", "finance"],
            ("kalshi", "d"): ["Politics"]}
    keep = gb.scope(texts, tags, texts)
    assert [k for k in texts if keep(k, set(), {})] == [("kalshi", "a"), ("polymarket", "c")]

def test_source_class():
    assert gb.source_class(dict(venue="kalshi", tags='["Companies"]')) == "company"
    assert gb.source_class(dict(venue="polymarket", tags='["earnings", "finance"]')) == "company"
    assert gb.source_class(dict(venue="polymarket", tags='["geopolitics", "earnings"]')) == "core"

class G:   # the bits of api.Graph that sector_links / exposures_of read
    def __init__(self, nodes, edges):
        self.nodes, self.out, self.inc = nodes, defaultdict(list), defaultdict(list)
        for e in edges: self.out[e["src"]].append(e); self.inc[e["dst"]].append(e)

def test_sector_links_and_exposures():
    import api
    nodes = {"q:1": dict(type="question", name="Tesla deliveries", attrs={}),
             "iss:Q478214": dict(type="issuer", name="Tesla", attrs=dict(sector="Consumer Discretionary"))}
    about = dict(src="q:1", dst="iss:Q478214", type="about", sign=1.0, weight=0.5, attrs=dict(channel="earnings"), note="m")
    g = G(nodes, [about])
    links = factors.sector_links(g)
    code = next(c for c, s in factors.SECTORS.items() if s["name"] == "Consumer Discretionary")
    assert links["q:1"][code]["source"] == "rule" and links["q:1"][code]["sign"] == 1
    g.linked = lambda ent: []
    ex = api.Graph.exposures_of(g, "iss:Q478214")
    assert ex["q:1"]["score"] == 0.5 and ex["q:1"]["impact"] == 0.5 and ex["q:1"]["paths"][0]["role"] == "company"
