import importlib.util, sys, json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

path = Path(__file__).resolve().parents[1] / "hindsight_api/engine/retain/source_context.py"
spec = importlib.util.spec_from_file_location("boundary_free_thread_check", path)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
assert not sys._is_gil_enabled(), "Free-threading must remain enabled"
chunks = ["prior", "current", "other"]
with ThreadPoolExecutor(max_workers=8) as pool:
    results = list(pool.map(lambda _: module.contexts(chunks, owners=[0, 0, 1]), range(64)))
assert all("prior" in r[1] and "other" not in r[1] for r in results)
assert chunks == ["prior", "current", "other"]
print(
    json.dumps(
        {"python": sys.version.split()[0], "gil_enabled": sys._is_gil_enabled(), "concurrent_cases": 64, "passed": True}
    )
)
