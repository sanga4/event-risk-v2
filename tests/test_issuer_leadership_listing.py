"""A dashboard's ranking limit must not hide directly linked leadership contracts."""
from types import SimpleNamespace
from unittest.mock import patch

import api


def test_ranking_limit_does_not_truncate_direct_leadership_contracts():
    iid = "iss:Q182477"
    g = SimpleNamespace(nodes={iid: {"attrs": {}}}, issuer_events={}, loadings={}, events_run=None,
                        ref=lambda i: {"id": i, "name": "NVIDIA", "type": "issuer"})
    direct = [{"id": "q:" + str(i), "move": None} for i in range(3)]
    with patch.object(api, "graph", return_value=g), \
         patch.object(api, "get_issuer_id", return_value=iid), \
         patch.object(api, "primary_security", return_value={"ticker": "NVDA"}), \
         patch.object(api, "event_rows", return_value=[]), \
         patch.object(api, "issuer_leadership_rows", return_value=direct), \
         patch.object(api, "live_total", return_value=3):
        result = api.dashboard_issuer("NVIDIA", limit=1, rank="sensitivity", cur=None)
    assert result["leadership_count"] == 3
    assert len(result["leadership"]) == 3
