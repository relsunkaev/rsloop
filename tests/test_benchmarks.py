import json
import sys
import unittest
from pathlib import Path
from unittest import mock


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

    def test_profile_runtime_enables_onearg_channel_for_rsloop_children(self) -> None:
        spec = loops.BENCHMARKS["call_soon"]

        with mock.patch.object(loops.subprocess, "run") as run:
            run.return_value = mock.Mock(
                stdout=json.dumps(
                    {
                        "results": [
                            {
                                "loop": "rsloop",
                                "benchmark": spec.name,
                                "category": spec.category,
                                "description": spec.description,
                                "iterations": 1,
                                "repeats": 1,
                                "unit": spec.unit,
                                "samples": [1.0],
                            }
                        ]
                    }
                ),
                stderr="",
            )

            loops.run_once_isolated(
                "rsloop",
                spec,
                iterations=1,
                profile_runtime=True,
                profile_stream=False,
                profile_python_streams=False,
                profile_python_cpu=False,
                profile_app_phases=False,
                child_retries=0,
            )

        env = run.call_args.kwargs["env"]
        self.assertEqual(env["RSLOOP_PROFILE_SCHED_JSON"], "1")
        self.assertEqual(env["RSLOOP_PROFILE_ONEARG_JSON"], "1")


if __name__ == "__main__":
    unittest.main()
