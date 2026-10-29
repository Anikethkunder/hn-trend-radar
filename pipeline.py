"""HN Trend Radar - hourly data pipeline.

extract (Algolia HN API) -> transform (silver) -> quality gate + data contract
-> load (append-only history) -> gold (SQL analytics) -> JSON for the website.

Env: INJECT_FAULT=none|extract|bad_data|schema_drift (demo)  MOCK=1 (offline data)  MOCK_NOW=<iso> (testing)
"""
import csv, json, math, os, random, re, sqlite3, sys, time, urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).parent
DATA, REP = ROOT / "data", ROOT / "reports"
API = "https://hn.algolia.com/api/v1/search?tags=front_page&hitsPerPage=60"
FAULT = os.getenv("INJECT_FAULT", "none")
MOCK = os.getenv("MOCK") == "1"
CONTRACT = {"objectID", "title", "points", "num_comments", "created_at", "author"}   # data contract
KEEP_DAYS = 7
TS = "%Y-%m-%dT%H:%MZ"

STOP = set("""a an the and or but if of to in on at for from with by as is are was were be been it its this that these those
you your we our they their i my me he she his her not no yes how why what when where who which will can could should would
do does did done have has had about into over under than then so just more most new now out up down off via vs per
show ask tell hn pdf video audio""".split())
SHORT = {"ai", "go", "ml", "vr", "c#", "os"}
TOPICS = {
    "AI & ML": set("ai llm llms gpt openai claude anthropic gemini model models neural ml agent agents chatgpt copilot diffusion".split()),
    "Security & Privacy": set("security vulnerability breach hack hacked exploit malware cve privacy encryption leak ransomware".split()),
    "Programming": set("rust python javascript typescript go golang java compiler programming code linux kernel api database sql postgres sqlite git".split()),
    "Business & Startups": set("startup funding yc company acquisition layoffs ipo pricing revenue founder market".split()),
    "Science & Hardware": set("space nasa physics chip cpu gpu battery quantum biology research study brain energy".split()),
}


def utcnow():
    v = os.getenv("MOCK_NOW")
    return datetime.fromisoformat(v).replace(tzinfo=timezone.utc) if v else datetime.now(timezone.utc)


def keywords(title):
    out = set()
    for w in re.findall(r"[a-z][a-z0-9+#.\-]*", (title or "").lower()):
        w = w.strip(".-")
        if (len(w) >= 3 or w in SHORT) and w not in STOP and not w.isdigit():
            out.add(w)
    return sorted(out)


def topic_of(kws):
    best = max(TOPICS, key=lambda t: len(TOPICS[t] & set(kws)))
    return best if TOPICS[best] & set(kws) else "General"


# ---------- Extract ----------
def mock_hits(now):
    H = int(now.timestamp() // 3600)
    names = ["Rust compiler speeds up builds", "New LLM beats benchmark", "Postgres tips every dev should know", "Critical CVE in popular library",
             "Startup raises funding for AI agents", "Quantum chip milestone", "Linux kernel release notes", "Python 3 performance work",
             "Privacy breach hits major company", "Open source database launches", "GPU shortage easing", "OpenAI model update"]
    hits = []
    for k in range(60):
        age, sid = k // 3, (H - k // 3) * 10 + k % 3
        r = random.Random(sid)
        hits.append({"objectID": str(sid), "title": f"{names[sid % len(names)]} ({sid % 97})", "url": f"https://site{sid % 9}.example.com/p/{sid}",
                     "author": f"user{sid % 31}", "points": int(160 * math.exp(-abs(age - 3) / 4) + r.randint(0, 40) + age * 6),
                     "num_comments": r.randint(5, 200),
                     "created_at": (now - timedelta(hours=age)).strftime("%Y-%m-%dT%H:00:00.000Z")})
    return hits


def fetch(now):
    if FAULT == "extract":
        raise RuntimeError("Injected fault: HN API returned HTTP 503")
    if MOCK:
        hits = mock_hits(now)
    else:
        last = None
        for i in range(3):
            try:
                req = urllib.request.Request(API, headers={"User-Agent": "hn-trend-radar/1.0"})
                with urllib.request.urlopen(req, timeout=20) as r:
                    hits = json.load(r)["hits"]; break
            except Exception as e:
                last = e; time.sleep(2 * (i + 1))
        else:
            raise RuntimeError(f"HN API failed after 3 attempts: {last}")
    if FAULT == "schema_drift":
        hits[0].pop("title", None)   # simulate the provider silently changing its schema
    return hits


# ---------- Transform (silver) ----------
def transform(hits):
    rows = []
    for i, h in enumerate(hits, 1):
        kws = keywords(h.get("title"))
        dom = urlparse(h.get("url") or "").netloc.lower().removeprefix("www.") or "news.ycombinator.com"
        rows.append({"id": str(h.get("objectID")), "rank": i, "title": h.get("title"), "url": h.get("url") or "", "domain": dom,
                     "author": h.get("author"), "points": h.get("points"), "comments": h.get("num_comments"),
                     "created_at": h.get("created_at"), "keywords": kws, "topic": topic_of(kws)})
    if FAULT == "bad_data" and len(rows) > 2:
        rows[0]["points"] = -50; rows[1]["id"] = rows[2]["id"]
    return rows


# ---------- Data quality ----------
def quality(rows, hits, now):
    checks = []
    add = lambda n, ok, d: checks.append({"check": n, "passed": bool(ok), "detail": d})
    missing = sorted({k for h in hits for k in CONTRACT - h.keys()})
    add("data_contract", not missing, "all expected fields present" if not missing else f"missing field(s): {', '.join(missing)}")
    add("row_count", len(rows) >= 30, f"{len(rows)} stories (minimum 30)")
    for f in ["id", "title", "points", "comments"]:
        n = sum(r[f] in (None, "") for r in rows)
        add(f"not_null:{f}", n == 0, f"{n} null value(s)")
    bad = [r["id"] for r in rows if isinstance(r["points"], (int, float)) and not 0 <= r["points"] <= 10000]
    add("range:points", not bad, "points within 0..10000" + (f"; bad ids: {', '.join(bad[:3])}" if bad else ""))
    add("unique_id", len({r["id"] for r in rows}) == len(rows), "story ids are unique")
    try:
        newest = max(datetime.fromisoformat(r["created_at"].replace("Z", "+00:00")) for r in rows if r["created_at"])
        age = (now - newest).total_seconds() / 3600
        add("freshness", age <= 12, f"newest story is {age:.1f} h old (max 12)")
    except Exception as e:
        add("freshness", False, f"cannot compute freshness: {e}")
    return checks


# ---------- Load (append-only history, pruned to 7 days) ----------
def load(rows, now):
    DATA.mkdir(exist_ok=True)
    sp, stp, ts = DATA / "snapshots.csv", DATA / "stories.json", now.strftime(TS)
    cutoff = (now - timedelta(days=KEEP_DAYS)).strftime(TS)
    snaps = []
    if sp.exists():
        with open(sp, newline="") as f:
            snaps = [r for r in csv.DictReader(f) if r["ts"] >= cutoff and r["ts"] != ts]   # re-run safe
    snaps += [{"ts": ts, "id": r["id"], "rank": r["rank"], "points": r["points"], "comments": r["comments"]} for r in rows]
    with open(sp, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["ts", "id", "rank", "points", "comments"]); w.writeheader(); w.writerows(snaps)
    stories = json.loads(stp.read_text()) if stp.exists() else {}
    for r in rows:
        stories[r["id"]] = {k: r[k] for k in ("title", "url", "domain", "author", "created_at", "keywords", "topic")}
    keep = {s["id"] for s in snaps}
    stp.write_text(json.dumps({k: v for k, v in stories.items() if k in keep}, sort_keys=True, separators=(",", ":")))
    return len(snaps), len(keep)


# ---------- Gold (SQL analytics) ----------
def gold(now):
    con = sqlite3.connect(":memory:"); con.row_factory = sqlite3.Row
    con.executescript("""CREATE TABLE snapshots(ts TEXT, id TEXT, rank INT, points INT, comments INT);
      CREATE TABLE stories(id TEXT PRIMARY KEY, title TEXT, url TEXT, domain TEXT, author TEXT, created_at TEXT, topic TEXT);
      CREATE TABLE story_kw(id TEXT, kw TEXT);""")
    with open(DATA / "snapshots.csv", newline="") as f:
        con.executemany("INSERT INTO snapshots VALUES (?,?,?,?,?)", [(r["ts"], r["id"], int(r["rank"]), int(r["points"]), int(r["comments"])) for r in csv.DictReader(f)])
    for sid, s in json.loads((DATA / "stories.json").read_text()).items():
        con.execute("INSERT INTO stories VALUES (?,?,?,?,?,?,?)", (sid, s["title"], s["url"], s["domain"], s["author"], s["created_at"], s["topic"]))
        con.executemany("INSERT INTO story_kw VALUES (?,?)", [(sid, k) for k in s["keywords"]])
    q = lambda sql, *a: [dict(r) for r in con.execute(sql, a)]
    latest = con.execute("SELECT MAX(ts) FROM snapshots").fetchone()[0]
    lt = datetime.strptime(latest, TS).replace(tzinfo=timezone.utc)
    base = con.execute("SELECT MAX(ts) FROM snapshots WHERE ts<=?", ((lt - timedelta(hours=6)).strftime(TS),)).fetchone()[0] \
        or con.execute("SELECT MIN(ts) FROM snapshots WHERE ts<?", (latest,)).fetchone()[0]
    base_h = round((lt - datetime.strptime(base, TS).replace(tzinfo=timezone.utc)).total_seconds() / 3600, 1) if base else 0

    movers = q("""SELECT l.id, st.title, st.url, st.domain, l.rank, l.points, l.comments,
                  l.points - COALESCE(b.points, l.points) AS gain
                  FROM snapshots l JOIN stories st ON st.id = l.id
                  LEFT JOIN snapshots b ON b.id = l.id AND b.ts = ?
                  WHERE l.ts = ? ORDER BY gain DESC, l.points DESC LIMIT 8""", base, latest)
    kw_sql = """SELECT k.kw, COUNT(*) n, SUM(s.points) pts FROM snapshots s JOIN story_kw k ON k.id = s.id
                WHERE s.ts = ? GROUP BY k.kw ORDER BY n DESC, pts DESC LIMIT 12"""
    prev = {r["kw"]: r["pts"] for r in q(kw_sql.replace("LIMIT 12", ""), base)} if base else {}
    kws = q(kw_sql, latest)
    for k in kws:
        p = prev.get(k["kw"])
        k["momentum"] = round(100 * (k["pts"] - p) / p) if p else None
    topics = q("""SELECT st.topic, SUM(s.points) pts, COUNT(*) n FROM snapshots s JOIN stories st ON st.id = s.id
                  WHERE s.ts = ? GROUP BY st.topic ORDER BY pts DESC""", latest)
    domains = q("""SELECT st.domain, COUNT(DISTINCT s.id) n, MAX(s.points) top FROM snapshots s JOIN stories st ON st.id = s.id
                   WHERE s.ts >= ? AND st.domain != 'news.ycombinator.com' GROUP BY st.domain ORDER BY n DESC, top DESC LIMIT 8""",
                (lt - timedelta(hours=24)).strftime(TS))
    new_n = q("SELECT COUNT(*) c FROM snapshots WHERE ts=? AND id NOT IN (SELECT id FROM snapshots WHERE ts=?)", latest, base)[0]["c"] if base else None
    st = q("""SELECT (SELECT COUNT(*) FROM stories) stories, (SELECT COUNT(DISTINCT ts) FROM snapshots) snapshots,
              (SELECT SUM(points) FROM snapshots WHERE ts=?) points, (SELECT MIN(ts) FROM snapshots) first""", latest)[0]
    out = {"generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "latest_ts": latest, "baseline_hours": base_h,
           "stats": {"stories": st["stories"], "snapshots": st["snapshots"], "points": st["points"], "new_entries": new_n,
                     "history_hours": round((lt - datetime.strptime(st["first"], TS).replace(tzinfo=timezone.utc)).total_seconds() / 3600, 1)},
           "keywords": kws, "movers": movers, "topics": topics, "domains": domains}
    REP.mkdir(exist_ok=True)
    (REP / "gold.json").write_text(json.dumps(out, indent=1))
    return out


# ---------- Orchestration ----------
def main():
    now = utcnow()
    repo, rid = os.getenv("GITHUB_REPOSITORY", "local"), os.getenv("GITHUB_RUN_ID", "local")
    report = {"run_id": rid, "run_url": f"https://github.com/{repo}/actions/runs/{rid}", "trigger": os.getenv("GITHUB_EVENT_NAME", "local"),
              "fault_injected": FAULT, "started_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "stages": [], "dq": [], "error": None}
    ctx = {}

    def s_extract():
        ctx["hits"] = fetch(now); return {"rows_out": len(ctx["hits"]), "detail": "1 API call"}

    def s_transform():
        ctx["rows"] = transform(ctx["hits"]); return {"rows_in": len(ctx["hits"]), "rows_out": len(ctx["rows"]), "detail": "keywords + topics + domains"}

    def s_quality():
        report["dq"] = quality(ctx["rows"], ctx["hits"], now)
        bad = [c["check"] for c in report["dq"] if not c["passed"]]
        if bad: raise RuntimeError(f"{len(bad)} quality check(s) failed: {', '.join(bad)}")
        return {"rows_in": len(ctx["rows"]), "rows_out": len(ctx["rows"]), "detail": f"{len(report['dq'])} checks passed"}

    def s_load():
        report["snapshots_rows"], report["stories_tracked"] = load(ctx["rows"], now); report["rows_loaded"] = len(ctx["rows"])
        return {"rows_in": len(ctx["rows"]), "rows_out": len(ctx["rows"]), "detail": f"history now {report['snapshots_rows']} rows"}

    def s_gold():
        g = gold(now); return {"detail": f"{len(g['keywords'])} trends, {len(g['movers'])} movers"}

    failed = False
    for name, fn in [("extract", s_extract), ("transform", s_transform), ("quality", s_quality), ("load", s_load), ("gold", s_gold)]:
        if failed:
            report["stages"].append({"name": name, "status": "skipped"}); continue
        t0, st = time.time(), {"name": name}
        try:
            st.update(fn()); st["status"] = "success"
        except Exception as e:
            st.update(status="failed", detail=str(e)); report["error"] = f"{name}: {e}"; failed = True
        st["duration_s"] = round(time.time() - t0, 2); report["stages"].append(st)
        print(f"[{st['status'].upper():7}] {name:<9} {st.get('detail', '')}")

    report["status"] = "failed" if failed else "success"
    report["finished_at"] = (now + timedelta(seconds=sum(s.get("duration_s", 0) for s in report["stages"]))).strftime("%Y-%m-%dT%H:%M:%SZ")
    report["duration_s"] = round(sum(s.get("duration_s", 0) for s in report["stages"]), 2)
    REP.mkdir(exist_ok=True)
    (REP / "latest_run.json").write_text(json.dumps(report, indent=1))
    hp = REP / "run_history.json"
    hist = json.loads(hp.read_text()) if hp.exists() else []
    hp.write_text(json.dumps((hist + [report])[-100:], indent=1))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
