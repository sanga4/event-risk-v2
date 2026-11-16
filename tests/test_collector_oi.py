"""Polymarket Data API v2 contract, batching and missing-data handling; no network or database."""
import unittest
from unittest.mock import patch

import requests

import collector


def record(condition):
    return {"contract": {"condition_id": condition}, "quote": {"open_interest": None}}


def http_error(status):
    response = requests.Response()
    response.status_code = status
    return requests.HTTPError(response=response)


class PolymarketOpenInterestTests(unittest.TestCase):
    def test_v2_contract_preserves_zero_and_missing_values(self):
        recs = [record(c) for c in ("a", "b", "c", "d")]
        response = {"data": [{"condition_id": "a", "value": 123.45},
                             {"condition_id": "b", "value": 0},
                             {"condition_id": "c", "value": None},
                             {"condition_id": "unrequested", "value": 999}]}
        with patch.object(collector, "get", return_value=response) as get:
            coverage = collector.poly_oi(recs)
        get.assert_called_once_with(collector.DATA + "/v2/oi", {"condition": "a,b,c,d"})
        self.assertEqual([r["quote"]["open_interest"] for r in recs], [123.45, 0.0, None, None])
        self.assertEqual(coverage, {"requested": 4, "received": 2, "missing": 2, "units": "USDC"})

    def test_batches_are_limited_to_twenty_distinct_conditions(self):
        recs = [record(str(i)) for i in range(21)]
        with patch.object(collector, "get", return_value={"data": []}) as get:
            coverage = collector.poly_oi(recs)
        batches = [call.args[1]["condition"].split(",") for call in get.call_args_list]
        self.assertEqual([len(batch) for batch in batches], [20, 1])
        self.assertEqual([c for batch in batches for c in batch], [str(i) for i in range(21)])
        self.assertEqual(coverage["missing"], 21)

    def test_no_conditions_never_requests_global_open_interest(self):
        with patch.object(collector, "get") as get:
            self.assertEqual(collector.poly_oi([])["requested"], 0)
            self.assertEqual(collector.poly_oi([record(None)])["requested"], 0)
        get.assert_not_called()

    def test_blocked_batches_split_and_unavailable_market_stays_null(self):
        def get(url, params):
            if params["condition"] in ("a,b", "b"):
                raise http_error(403)
            return {"data": [{"condition_id": "a", "value": 12}]}

        recs = [record("a"), record("b")]
        with patch.object(collector, "get", side_effect=get):
            coverage = collector.poly_oi(recs)
        self.assertEqual([r["quote"]["open_interest"] for r in recs], [12.0, None])
        self.assertEqual(coverage["missing"], 1)

    def test_upstream_errors_are_not_hidden(self):
        for status in (400, 410, 429, 503):
            with self.subTest(status=status):
                with patch.object(collector, "get", side_effect=http_error(status)):
                    with self.assertRaises(requests.HTTPError):
                        collector.poly_oi([record("a")])

    def test_malformed_or_legacy_response_is_not_silent_success(self):
        for response in ({}, {"data": None}, {"data": {}}, [{"market": "a", "value": 5}]):
            with self.subTest(response=response):
                with patch.object(collector, "get", return_value=response):
                    with self.assertRaisesRegex(ValueError, "data array"):
                        collector.poly_oi([record("a")])


if __name__ == "__main__":
    unittest.main()
