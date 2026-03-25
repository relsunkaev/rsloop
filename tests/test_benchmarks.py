import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "benchmarks"))

import loops  # noqa: E402


class BenchmarkProfileParsingTests(unittest.TestCase):
    def test_parse_runtime_profile_preserves_all_run_channels(self) -> None:
        stderr = "\n".join(
            [
                "RSLOOP_PROFILE_SCHED_JSON " + json.dumps({"iterations": 1}),
                "RSLOOP_PROFILE_STREAM_JSON " + json.dumps({"events": [], "dropped": 0}),
                "RSLOOP_PROFILE_ONEARG_JSON " + json.dumps({"callbacks": []}),
                "BENCH_PROFILE_PY_STREAM_JSON " + json.dumps({"totals": {}}),
                "BENCH_PROFILE_CPU_JSON " + json.dumps({"sample_count": 4}),
                "BENCH_PROFILE_APP_PHASE_JSON "
                + json.dumps({"phases": {"http.client.connect": {"count": 1}}}),
            ]
        )

        profile = loops.parse_runtime_profile(stderr)

        self.assertIsNotNone(profile)
        assert profile is not None
        self.assertIn("scheduler_runs", profile)
        self.assertIn("stream_runs", profile)
        self.assertIn("onearg_runs", profile)
        self.assertIn("python_stream_runs", profile)
        self.assertIn("cpu_runs", profile)
        self.assertIn("app_phase_runs", profile)
        self.assertNotIn("scheduler", profile)
        self.assertNotIn("stream", profile)


if __name__ == "__main__":
    unittest.main()
