import json
import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "benchmarks"))

import loops  # noqa: E402
import native_sample  # noqa: E402


class BenchmarkProfileParsingTests(unittest.TestCase):
    def test_get_loop_factory_uses_rsloop_factory(self) -> None:
        with mock.patch.object(loops.rsloop, "new_event_loop", return_value=object()) as new_loop:
            factory = loops.get_loop_factory("rsloop")
            factory()
        new_loop.assert_called_once_with()

    def test_parse_runtime_profile_preserves_all_run_channels(self) -> None:
        stderr = "\n".join(
            [
                "RSLOOP_PROFILE_SCHED_JSON " + json.dumps({"iterations": 1}),
                "RSLOOP_PROFILE_STREAM_JSON " + json.dumps({"events": [], "dropped": 0}),
                "RSLOOP_PROFILE_ONEARG_JSON " + json.dumps({"callbacks": []}),
                "BENCH_PROFILE_PY_STREAM_JSON " + json.dumps({"totals": {}}),
                "BENCH_PROFILE_SSLPROTO_JSON " + json.dumps({"methods": []}),
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
        self.assertIn("sslproto_runs", profile)
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
                profile_sslproto=False,
                profile_python_cpu=False,
                profile_app_phases=False,
                child_retries=0,
            )

        env = run.call_args.kwargs["env"]
        cmd = run.call_args.args[0]
        self.assertNotIn("--rsloop-mode", cmd)
        self.assertEqual(env["RSLOOP_PROFILE_SCHED_JSON"], "1")
        self.assertEqual(env["RSLOOP_PROFILE_ONEARG_JSON"], "1")

    def test_profile_sslproto_enables_sslproto_channel_for_children(self) -> None:
        spec = loops.BENCHMARKS["tls_http1_keepalive"]

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
                profile_runtime=False,
                profile_stream=False,
                profile_python_streams=False,
                profile_sslproto=True,
                profile_python_cpu=False,
                profile_app_phases=False,
                child_retries=0,
            )

        env = run.call_args.kwargs["env"]
        cmd = run.call_args.args[0]
        self.assertNotIn("--rsloop-mode", cmd)
        self.assertEqual(env["BENCH_PROFILE_SSLPROTO_JSON"], "1")

    def test_native_sample_parses_call_graph_frames(self) -> None:
        path = ROOT / "benchmarks" / "out" / "sample-parser-fixture.txt"
        path.write_text(
            "\n".join(
                [
                    "Call graph:",
                    "    + 10 task_wakeup  (in _asyncio.cpython-314-darwin.so) + 384",
                    "    + ! 7 _ssl__SSLSocket_read  (in _ssl.cpython-314-darwin.so) + 824",
                    "    + ! : 7 SSL_read_ex  (in libssl.3.dylib) + 12",
                    "",
                    "Total number in stack (recursive counted multiple, when >=5):",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        self.addCleanup(path.unlink)

        frames = native_sample.parse_sample_frames(path)

        self.assertEqual(frames[0]["frame"], "task_wakeup")
        self.assertEqual(frames[0]["count"], 10)
        self.assertEqual(frames[1]["frame"], "_ssl__SSLSocket_read")
        self.assertEqual(frames[1]["count"], 7)


if __name__ == "__main__":
    unittest.main()
