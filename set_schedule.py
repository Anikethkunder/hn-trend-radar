import json, re
from pathlib import Path

cron = json.loads(Path("config.json").read_text())["cron"]
wf = Path(".github/workflows/pipeline.yml")
wf.write_text(re.sub(r'(- cron: ")[^"]*(")', rf'\g<1>{cron}\g<2>', wf.read_text()))
print(f"Schedule set to: {cron} (UTC). Now commit and push.")
