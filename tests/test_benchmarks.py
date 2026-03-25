import json
import sys
import types
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

    def test_sslproto_profiler_wraps_only_overrides_on_rsloop_subclass(self) -> None:
        class FakeState:
            name = "WRAPPED"

        class FakeBase:
            def __init__(self) -> None:
                self._state = FakeState()
                self._app_protocol_is_buffer = False

            def buffer_updated(self, nbytes: int) -> None:
                return None

            def _do_read(self) -> None:
                return None

            def _do_read__copied(self) -> None:
                return None

            def _do_read__buffered(self) -> None:
                return None

            def _process_outgoing(self) -> None:
                return None

            def _write_appdata(self, chunks: tuple[bytes, ...]) -> None:
                return None

            def _control_ssl_reading(self) -> None:
                return None

            def _on_handshake_complete(self, exc: BaseException | None) -> None:
                return None

        class FakeRsloop(FakeBase):
            def _do_read(self) -> None:
                return None

            def _process_outgoing(self) -> None:
                return None

        fake_module = types.ModuleType("rsloop.sslproto")
        fake_module.RsloopSSLProtocol = FakeRsloop
        profiler = loops.SslProtoProfiler()

        with mock.patch.object(loops._sslproto, "SSLProtocol", FakeBase):
            with mock.patch.dict(sys.modules, {"rsloop.sslproto": fake_module}):
                profiler.install()
                inst = FakeRsloop()
                inst.buffer_updated(1)
                inst._do_read()
                inst._process_outgoing()

        methods = {bucket["method"]: bucket for bucket in profiler._methods.values()}
        self.assertEqual(methods["buffer_updated"]["count"], 1)
        self.assertEqual(methods["_do_read"]["count"], 1)
        self.assertEqual(methods["_process_outgoing"]["count"], 1)
        self.assertEqual(methods["_do_read"]["owner"], "FakeRsloop")
        self.assertEqual(methods["buffer_updated"]["owner"], "FakeBase")

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
