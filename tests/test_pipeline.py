import sys, tempfile, unittest
from datetime import timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import pipeline as p

NOW = p.utcnow().replace(minute=0, second=0, microsecond=0)


def hits(points=100):
    return [{"objectID": str(i), "title": f"Rust and LLM story {i}", "url": f"https://www.site{i}.com/x", "author": "a", "points": points + i,
             "num_comments": 3, "created_at": (NOW - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S.000Z")} for i in range(40)]


class T(unittest.TestCase):
    def test_keywords_and_topic(self):
        k = p.keywords("Show HN: A new Rust LLM compiler")
        self.assertIn("rust", k); self.assertNotIn("the", k); self.assertNotIn("show", k)
        self.assertEqual(p.topic_of(["llm", "gpt"]), "AI & ML")

    def test_domain_extracted(self):
        self.assertEqual(p.transform(hits())[3]["domain"], "site3.com")

    def test_quality_passes_on_good_data(self):
        h = hits(); self.assertTrue(all(c["passed"] for c in p.quality(p.transform(h), h, NOW)))

    def test_quality_catches_bad_points_and_duplicates(self):
        h = hits(); r = p.transform(h); r[0]["points"] = -5; r[1]["id"] = r[2]["id"]
        failed = [c["check"] for c in p.quality(r, h, NOW) if not c["passed"]]
        self.assertIn("range:points", failed); self.assertIn("unique_id", failed)

    def test_data_contract_detects_schema_drift(self):
        h = hits(); h[0].pop("title")
        self.assertIn("data_contract", [c["check"] for c in p.quality(p.transform(h), h, NOW) if not c["passed"]])

    def test_gold_computes_point_gain_and_is_rerun_safe(self):
        with tempfile.TemporaryDirectory() as d:
            p.DATA, p.REP = Path(d) / "data", Path(d) / "reports"
            t0, t1 = NOW - timedelta(hours=2), NOW
            p.load(p.transform(hits(100)), t0)
            p.load(p.transform(hits(150)), t1); p.load(p.transform(hits(150)), t1)   # same snapshot twice
            g = p.gold(t1)
            self.assertEqual(g["stats"]["snapshots"], 2)
            self.assertEqual(g["movers"][0]["gain"], 50)
            self.assertIn("rust", [k["kw"] for k in g["keywords"]])


if __name__ == "__main__":
    unittest.main()
