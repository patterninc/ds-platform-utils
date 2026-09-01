"""Unit tests for the @remote_step machinery that can run without Snowflake.

The AST pass, the self proxy, the parquet codec and the globals rewriting are all pure -- and they
are the parts most likely to break silently, so they are worth pinning down locally.
"""

import io
import shutil
import sys
import zipfile
from pathlib import Path

import pandas as pd
import pytest

import ds_platform_utils.metaflow.external_compute
from ds_platform_utils.metaflow.external_compute import (
    SelfProxy,
    StepRef,
    analyze_self_access,
    bundle_paths,
    collect_path_globals,
    decode_value,
    discover_importable_packages,
    encode_value,
    execute_remote_step,
    rebind_globals,
)
from ds_platform_utils.metaflow.remote_runtime import await_job

CONSTANT = "module-level-constant"


def build_model():
    """Stand-in for a real model builder. Bodies below are parsed, not executed."""
    raise NotImplementedError


def transform(*args):
    """Stand-in used by parsed-only bodies."""
    raise NotImplementedError


inputs = None  # name referenced by a parsed-only body
SQL_DIR = None  # stands in for a flow's module-level Path; tests inject the real value


# ---------------------------------------------------------------------------
# AST analysis: ship only what the body touches
# ---------------------------------------------------------------------------


def test_analyze_separates_reads_from_writes():
    def body(self):
        total = self.raw_rows + self.offset
        self.result = total
        self.next(self.end)

    reads, writes = analyze_self_access(body)
    assert reads == ["offset", "raw_rows"]
    assert writes == ["result"]


def test_attribute_written_then_read_is_not_an_input():
    # The real case this was built for: reading back something the body just produced must not
    # make the decorator try to ship it from the flow.
    def body(self):
        self.model = build_model()
        self.score = self.model.evaluate()
        self.next(self.end)

    reads, writes = analyze_self_access(body)
    assert reads == []
    assert writes == ["model", "score"]


def test_attribute_read_then_written_is_still_an_input():
    def body(self):
        self.total = self.total + 1

    reads, writes = analyze_self_access(body)
    assert reads == ["total"]
    assert writes == ["total"]


def test_augmented_assignment_counts_as_both():
    def body(self):
        self.counter += 1

    reads, writes = analyze_self_access(body)
    assert reads == ["counter"]
    assert writes == ["counter"]


def test_next_target_is_not_treated_as_an_artifact():
    # `self.end` is a step reference, not data -- shipping it would fail, so it must not be a read.
    def body(self):
        self.next(self.end)

    reads, writes = analyze_self_access(body)
    assert reads == []
    assert writes == []


def test_branch_targets_are_all_excluded():
    def body(self):
        self.next(self.left, self.right)

    reads, _ = analyze_self_access(body)
    assert reads == []


def test_metaflow_methods_are_not_shipped():
    def body(self):
        self.merge_artifacts(inputs)
        self.next(self.end)

    reads, writes = analyze_self_access(body)
    assert reads == []  # `merge_artifacts` is a method, not an artifact


def test_foreach_input_is_shipped():
    # Inside a foreach, `self.input` holds the item being processed -- it is data the body needs,
    # so it must be shipped rather than treated as Metaflow plumbing.
    def body(self):
        self.result = transform(self.input, self.index)
        self.next(self.join)

    reads, writes = analyze_self_access(body)
    assert reads == ["index", "input"]
    assert writes == ["result"]


# ---------------------------------------------------------------------------
# The proxy
# ---------------------------------------------------------------------------


def test_proxy_reads_shipped_inputs_and_collects_writes():
    proxy = SelfProxy({"raw_rows": 10}, step_names=["end"])
    assert proxy.raw_rows == 10

    proxy.result = 99
    assert object.__getattribute__(proxy, "_data")["result"] == 99


def test_proxy_resolves_step_names_to_references():
    proxy = SelfProxy({}, step_names=["end", "join"])
    assert isinstance(proxy.end, StepRef)
    assert proxy.end.name == "end"


def test_proxy_records_next_without_executing_it():
    proxy = SelfProxy({}, step_names=["a", "b"])
    proxy.next(proxy.a, proxy.b, foreach="items")

    recorded = object.__getattribute__(proxy, "_next_call")
    assert recorded == {"steps": ["a", "b"], "kwargs": {"foreach": "items"}}


def test_proxy_raises_a_useful_error_for_unshipped_attributes():
    # The failure mode that matters: a dynamically-read artifact must not silently become a StepRef.
    proxy = SelfProxy({}, step_names=["end"])
    with pytest.raises(AttributeError, match="extra_inputs"):
        _ = proxy.never_shipped


# ---------------------------------------------------------------------------
# Parquet codec: survives a pandas version gap
# ---------------------------------------------------------------------------


def test_dataframe_round_trips_through_parquet():
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    encoded = encode_value(df)

    assert isinstance(encoded, dict)  # not a pickled frame
    pd.testing.assert_frame_equal(decode_value(encoded), df)


def test_non_dataframes_pass_through_untouched():
    for value in (42, "text", b"bytes", {"k": "v"}, [1, 2]):
        assert decode_value(encode_value(value)) == value


# ---------------------------------------------------------------------------
# Globals rewriting
# ---------------------------------------------------------------------------


def test_rebind_leaves_untouched_globals_alone():
    def body(self):
        return CONSTANT

    rebound = rebind_globals(body)
    assert rebound(None) == CONSTANT


def test_rebind_does_not_snapshot_current_when_unused():
    # Only referenced names are swapped, so a body that never mentions `current` must not drag
    # Metaflow's context (and its unpicklable innards) into the payload.
    def body(self):
        self.out = 1

    assert "current" not in rebind_globals(body).__globals__


# ---------------------------------------------------------------------------
# End-to-end, with the remote hop faked out
# ---------------------------------------------------------------------------


def test_execute_remote_step_runs_body_and_returns_writes():
    import cloudpickle

    def body(self):
        self.doubled = self.value * 2
        self.next(self.end)

    result = cloudpickle.loads(
        execute_remote_step(
            cloudpickle.dumps(body),
            encoded_inputs={"value": 21},
            write_names=["doubled"],
            step_names=["end"],
        )
    )

    assert result["writes"]["doubled"] == 42
    assert result["next"] == {"steps": ["end"], "kwargs": {}}


def test_execute_remote_step_round_trips_a_dataframe():
    import cloudpickle

    def body(self):
        self.scored = self.frame.assign(doubled=self.frame["a"] * 2)
        self.next(self.end)

    df = pd.DataFrame({"a": [1, 2, 3]})
    result = cloudpickle.loads(
        execute_remote_step(
            cloudpickle.dumps(body),
            encoded_inputs={"frame": encode_value(df)},
            write_names=["scored"],
            step_names=["end"],
        )
    )

    scored = decode_value(result["writes"]["scored"])
    assert scored["doubled"].tolist() == [2, 4, 6]


# ---------------------------------------------------------------------------
# Path bundling: files behind module-level Path globals travel with the job
# ---------------------------------------------------------------------------


def test_collect_path_globals_finds_referenced_paths(tmp_path):
    sql_dir = tmp_path / "sql"
    sql_dir.mkdir()
    (sql_dir / "q.sql").write_text("SELECT 1")

    def body(self):
        self.out = (SQL_DIR / "q.sql").read_text()

    body.__globals__["SQL_DIR"] = sql_dir
    assert collect_path_globals(body) == {"SQL_DIR": sql_dir}


def test_unreferenced_path_globals_are_not_bundled(tmp_path):
    other = tmp_path / "other"
    other.mkdir()

    def body(self):
        self.out = 1

    body.__globals__["OTHER_DIR"] = other
    assert collect_path_globals(body) == {}


def test_sql_file_survives_the_round_trip(tmp_path):
    # The marketshare pattern: SQL_DIR is a module-level Path and the body reads a file from it.
    # The Path ships fine on its own; without bundling, the file would not exist in the container.
    import cloudpickle

    sql_dir = tmp_path / "sql" / "f1_data_prep"
    sql_dir.mkdir(parents=True)
    (sql_dir / "keepa.sql").write_text("SELECT * FROM KEEPA")
    (sql_dir / "prices.sql").write_text("SELECT * FROM PRICES")

    def body(self):
        self.query = (SQL_DIR / "keepa.sql").read_text()
        self.next(self.end)

    body.__globals__["SQL_DIR"] = sql_dir
    bundle, mapping = bundle_paths(collect_path_globals(body))
    assert mapping == {"SQL_DIR": "SQL_DIR"}

    # Simulate the container: the original directory is gone.
    shutil.rmtree(tmp_path / "sql")

    result = cloudpickle.loads(
        execute_remote_step(
            cloudpickle.dumps(body),
            encoded_inputs={},
            write_names=["query"],
            step_names=["end"],
            path_bundle=bundle,
            path_map=mapping,
        )
    )
    assert result["writes"]["query"] == "SELECT * FROM KEEPA"


def test_bundle_skips_pycache(tmp_path):
    src = tmp_path / "configs"
    (src / "__pycache__").mkdir(parents=True)
    (src / "__pycache__" / "junk.pyc").write_bytes(b"\x00" * 64)
    (src / "default.yaml").write_text("a: 1")

    bundle, _ = bundle_paths({"CONFIG_DIR": src})
    with zipfile.ZipFile(io.BytesIO(bundle)) as archive:
        assert archive.namelist() == ["CONFIG_DIR/default.yaml"]


def test_oversized_bundle_fails_with_a_useful_message(tmp_path, monkeypatch):
    monkeypatch.setattr(ds_platform_utils.metaflow.external_compute, "MAX_PATH_BUNDLE_BYTES", 100)
    big = tmp_path / "data"
    big.mkdir()
    (big / "rows.csv").write_text("col\n" + "1234567890\n" * 500)

    with pytest.raises(RuntimeError, match="narrower directory"):
        bundle_paths({"DATA_DIR": big})


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------


def test_non_string_dict_keys_survive_the_result_boundary():
    # Snowflake's result protocol walks returned containers and calls k.startswith() on every dict
    # key, so an artifact like {7: 0.1} used to fail *after* the job had already succeeded.
    # Returning one opaque blob keeps user data away from that walk.
    import cloudpickle

    def body(self):
        self.label_balance = {7: 0.12, 14: 0.34}
        self.next(self.end)

    raw = execute_remote_step(
        cloudpickle.dumps(body),
        encoded_inputs={},
        write_names=["label_balance"],
        step_names=["end"],
    )

    assert isinstance(raw, bytes), "result must be opaque bytes, not a walkable container"
    result = cloudpickle.loads(raw)
    assert result["writes"]["label_balance"] == {7: 0.12, 14: 0.34}


def test_missing_path_global_warns_instead_of_silently_skipping(tmp_path, capsys):
    # A referenced Path that does not exist locally almost always means Metaflow's code package
    # left the files behind. Skipping quietly turns that into a FileNotFoundError inside the
    # container minutes later, so it has to be visible at submit time.
    def body(self):
        self.out = (SQL_DIR / "q.sql").read_text()

    body.__globals__["SQL_DIR"] = tmp_path / "does_not_exist"

    assert collect_path_globals(body) == {}
    warning = capsys.readouterr().out
    assert "does not exist here" in warning
    assert "--package-suffixes" in warning


# ---------------------------------------------------------------------------
# Container logs are replayed into the Metaflow step's own output
# ---------------------------------------------------------------------------


def test_discovers_every_package_beside_the_flow(tmp_path):
    # ads_utils, meridian and configs are all real packages in this repo, so shipping only
    # `helpers` would break any body importing from them.
    for package in ("helpers", "ads_utils", "meridian"):
        (tmp_path / package).mkdir()
        (tmp_path / package / "__init__.py").touch()
    # A namespace package: real code, no __init__.py. operations/excess-inventory does exactly
    # this with `from configs.configs import ...`, so it has to ship too.
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "configs.py").touch()

    (tmp_path / "sql").mkdir()  # data, not code -- no .py files at all
    (tmp_path / "sql" / "q.sql").touch()
    (tmp_path / "__pycache__").mkdir()
    (tmp_path / "__pycache__" / "junk.py").touch()
    (tmp_path / "flow.py").touch()

    import types as pytypes

    def source(self):
        self.out = 1

    # Own globals, not this module's. Assigning __file__ into `source.__globals__` would rewrite
    # the test module's own __file__ to a tmp_path that stops existing when the test ends.
    body = pytypes.FunctionType(source.__code__, {"__file__": str(tmp_path / "flow.py")}, "body")
    discovered = discover_importable_packages(body)

    assert sorted(name for _, name in discovered) == ["ads_utils", "configs", "helpers", "meridian"]
    assert all(Path(directory).is_dir() for directory, _ in discovered)


def test_discovery_falls_back_when_the_body_has_no_module_file():
    import types as pytypes

    def source(self):
        self.out = 1

    # A function with its own globals, rather than popping __file__ out of this module's. Doing
    # the latter corrupts every later test in the file: `source.__globals__` *is* the module dict.
    body = pytypes.FunctionType(source.__code__, {"__name__": "nowhere"}, "body")

    # Nothing to ship: ds_platform_utils is installed in the image, not uploaded per job.
    assert discover_importable_packages(body) == []


def test_current_stand_in_keeps_snapshot_truthful_and_swallows_cards():
    import types as pytypes

    from ds_platform_utils.metaflow.snowflake_access import CurrentStandIn

    stand_in = CurrentStandIn(pytypes.SimpleNamespace(is_production=True, run_id="42"))

    # is_production is what selects the prod vs dev schema, so it must survive the crossing.
    assert stand_in.is_production is True
    assert stand_in.run_id == "42"
    assert bool(stand_in) is True
    assert stand_in.not_captured is None  # missing fields must not raise
    stand_in.card.append("anything")  # cards cannot reach the task; swallowed


def test_bootstrap_is_a_noop_when_library_is_absent(monkeypatch):
    import builtins

    from ds_platform_utils.metaflow.snowflake_access import bootstrap_ds_platform_utils

    real_import = builtins.__import__

    def blocked(name, *args, **kwargs):
        if name.startswith("ds_platform_utils"):
            raise ImportError("not installed in this container")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocked)
    assert bootstrap_ds_platform_utils(None) == {
        "installed": False,
        "patched_current": 0,
        "patched_connection": 0,
    }


def test_container_connection_wrapper_ignores_close():
    # ds_platform_utils closes the connection when it finishes with it. That is correct when it
    # created the connection; here it is handed the container's shared session connection, so an
    # honest close() ends the session and the next library call fails with "Connection is closed".
    from ds_platform_utils.metaflow.snowflake_access import _NonClosingConnection

    class _RealConnection:
        def __init__(self):
            self.closed = False
            self.database = "PATTERN_DB"

        def close(self):
            self.closed = True

        def cursor(self):
            return "cursor"

    real = _RealConnection()
    wrapped = _NonClosingConnection(real)

    wrapped.close()
    assert real.closed is False, "close() must not reach the shared connection"

    # everything else still delegates
    assert wrapped.database == "PATTERN_DB"
    assert wrapped.cursor() == "cursor"

    with wrapped as ctx:
        assert ctx is wrapped
    assert real.closed is False, "leaving a with-block must not close it either"


# ---------------------------------------------------------------------------
# Live log streaming
# ---------------------------------------------------------------------------


class _StreamingJob:
    """A job handle whose logs grow over successive polls, then finish."""

    id = "train-1787576207-a1b2c3d4"
    terminal_statuses = frozenset({"Completed", "Failed", "Stopped"})

    def __init__(self, chunks, final_status="Completed"):
        self._chunks = list(chunks)
        self._final_status = final_status
        self._poll = 0

    @property
    def status(self):
        return "RUNNING" if self._poll < len(self._chunks) else self._final_status

    def get_logs(self):
        visible = "".join(self._chunks[: self._poll])
        self._poll += 1
        return visible

    def result(self):
        return b"payload"


def test_logs_are_printed_as_they_appear(capsys):
    # The point of streaming: output shows up while the job runs, not in one dump at the end.
    from ds_platform_utils.metaflow.remote_runtime import stream_job_logs

    job = _StreamingJob(["epoch 1\n", "epoch 2\n", "epoch 3\n"])
    assert stream_job_logs(job, poll_seconds=0) == "Completed"

    out = capsys.readouterr().out
    assert "epoch 1" in out and "epoch 2" in out and "epoch 3" in out
    assert out.count("epoch 2") == 1, "each line must be printed once, not re-printed every poll"


def test_streaming_survives_log_fetch_failures(capsys):
    # A container that has not started yet has no logs; that must not kill the job.
    from ds_platform_utils.metaflow.remote_runtime import stream_job_logs

    class _FlakyJob(_StreamingJob):
        def get_logs(self):
            self._poll += 1
            if self._poll < 2:
                raise ConnectionError("logs not ready")
            return "done work\n"

    assert stream_job_logs(_FlakyJob(["x", "y"]), poll_seconds=0) == "Completed"
    assert "done work" in capsys.readouterr().out


def test_streaming_reprints_if_the_log_source_resets(capsys):
    from ds_platform_utils.metaflow.remote_runtime import stream_job_logs

    class _ResettingJob(_StreamingJob):
        def get_logs(self):
            self._poll += 1
            return "a long first log\n" if self._poll == 1 else "short\n"

    stream_job_logs(_ResettingJob(["x", "y"]), poll_seconds=0)
    out = capsys.readouterr().out
    assert "a long first log" in out and "short" in out


def test_await_job_streams_then_returns_the_result(capsys):
    job = _StreamingJob(["fitting...\n", "done\n"])
    assert await_job(job, label="train", poll_seconds=0) == b"payload"

    out = capsys.readouterr().out
    assert "fitting..." in out and "done" in out
    assert "Completed" in out


def test_await_job_prints_logs_before_raising(capsys):
    # On failure the logs are the only explanation, so they must reach the step before the raise.

    class _FailingJob(_StreamingJob):
        def result(self):
            raise RuntimeError("payload blew up")

    with pytest.raises(RuntimeError, match="payload blew up"):
        await_job(_FailingJob(["traceback here\n"], final_status="Failed"), label="train", poll_seconds=0)

    out = capsys.readouterr().out
    assert "traceback here" in out
    assert "Failed" in out


def test_snowflake_identity_shapes_the_row_it_gets(monkeypatch):
    # The query needs Snowflake; the shaping does not. Labels are attached and values stringified
    # so the two routes can be compared field by field.
    import ds_platform_utils.metaflow as dsp
    from ds_platform_utils.metaflow.snowflake_access import snowflake_identity

    monkeypatch.setattr(
        dsp,
        "query_pandas_from_snowflake",
        lambda query: pd.DataFrame([{"role": "DS_ROLE", "snowflake_user": "SVC", "warehouse": None}]),
    )

    identity = snowflake_identity("metaflow step")
    assert identity["label"] == "metaflow step"
    assert identity["role"] == "DS_ROLE"
    assert identity["warehouse"] is None


# ---------------------------------------------------------------------------
# Python version coupling
# ---------------------------------------------------------------------------


def test_matching_python_minor_versions_pass():
    import sys

    from ds_platform_utils.metaflow.external_compute import assert_python_versions_match

    # Patch versions differ constantly (3.11.0 submitting, 3.11.14 in the container) and share
    # bytecode, so only major.minor is compared.
    assert_python_versions_match(sys.version_info[:2])


def test_mismatched_python_minor_version_is_refused():
    import sys

    from ds_platform_utils.metaflow.external_compute import assert_python_versions_match

    major, minor = sys.version_info[:2]
    with pytest.raises(RuntimeError, match="not portable across minor versions"):
        assert_python_versions_match((major, minor + 1))


def test_version_guard_names_both_versions():
    from ds_platform_utils.metaflow.external_compute import assert_python_versions_match

    with pytest.raises(RuntimeError) as exc:
        assert_python_versions_match((3, 9))

    message = str(exc.value)
    assert "3.9" in message and "PYTHON_VERSION" in message


# ---------------------------------------------------------------------------
# Environment bootstrap and resource selection
# ---------------------------------------------------------------------------


def test_packages_become_pip_requirements():
    from ds_platform_utils.metaflow.external_compute import requirements_from_packages

    assert requirements_from_packages({"xgboost": "2.0.3"}) == ["xgboost==2.0.3"]
    assert requirements_from_packages({"shap": ""}) == ["shap"]
    # A value that is already a specifier is used as written, not forced to ==
    assert requirements_from_packages({"pandas": ">=2.0,<3"}) == ["pandas>=2.0,<3"]
    assert requirements_from_packages(None) == []


# ---------------------------------------------------------------------------
# The backend seam
# ---------------------------------------------------------------------------


class _FakeHandle:
    """A JobHandle that runs the payload in-process, standing in for a compute service."""

    id = "FAKE_JOB_1"
    # Must be in `terminal_statuses`, or the log streamer polls this handle forever.
    status = "Completed"
    terminal_statuses = frozenset({"Completed", "Failed", "Stopped"})

    def __init__(self, call):
        from ds_platform_utils.metaflow.external_compute import execute_remote_step

        self._result = execute_remote_step(
            call.fn_bytes,
            call.inputs,
            call.write_names,
            call.step_names,
            call.path_bundle,
            call.path_map,
            call.python_version,
        )

    def get_logs(self):
        return "fake backend ran the body\n"

    def result(self):
        return self._result


class _FakeBackend:
    """Records what it was asked to run, then runs it locally."""

    name = "fake"

    def __init__(self):
        self.calls = []

    def describe(self, call):
        return f"fake backend, cpu={call.resources.cpu}"

    def submit(self, call):
        self.calls.append(call)
        return _FakeHandle(call)


def _fake_step(fn):
    """Mark a method the way Metaflow's @step does, so the decorator can discover step names."""
    fn.is_step = True
    return fn


class _FakeFlow:
    """The smallest thing that looks like a FlowSpec to the decorator.

    The step methods matter: the decorator reads them off the class so the proxy can resolve
    `self.end` in `self.next(...)` as a step reference rather than a missing artifact.
    """

    def __init__(self, **artifacts):
        self.__dict__.update(artifacts)
        self.transitions = []

    @_fake_step
    def train(self):
        """A step, for name discovery only."""

    @_fake_step
    def step_body(self):
        """A step, for name discovery only."""

    @_fake_step
    def end(self):
        """A step, for name discovery only."""

    def next(self, *steps, **kwargs):
        self.transitions.append(([getattr(step, "__name__", step) for step in steps], kwargs))


def test_decorator_runs_against_any_backend():
    # The point of the seam: nothing in the decorator is Snowflake-specific. This exercises the
    # whole path -- AST pass, pickling, proxy, writes copied back -- with no Snowflake installed,
    # authenticated or reachable.
    from ds_platform_utils.metaflow.external_compute import remote_step

    backend = _FakeBackend()

    @remote_step(backend, cpu=2)
    def train(self):
        self.doubled = self.value * 2
        self.next(self.end)

    flow = _FakeFlow(value=21)
    train(flow)

    assert flow.doubled == 42, "writes must be copied back onto the flow"
    assert backend.calls[0].step_name == "train"
    assert backend.calls[0].resources.cpu == 2
    assert backend.calls[0].write_names == ["doubled"]
    assert "remote_runtimes" in flow.__dict__


def test_backend_receives_a_plain_remote_call():
    # RemoteCall is deliberately inert data: a backend can inspect or serialise it without needing
    # any of the decorator's context.
    from ds_platform_utils.metaflow.compute_backends import RemoteCall
    from ds_platform_utils.metaflow.external_compute import remote_step

    backend = _FakeBackend()

    @remote_step(backend)
    def step_body(self):
        self.out = 1
        self.next(self.end)

    step_body(_FakeFlow())
    call = backend.calls[0]

    assert isinstance(call, RemoteCall)
    assert isinstance(call.fn_bytes, bytes)
    assert call.python_version == sys.version_info[:2]

    # Backend-specific requirements are the backend's business. ds-platform-utils is added by the
    # Snowflake backend, because it is useless where there is no Snowflake identity.
    assert not any("ds-platform-utils" in requirement for requirement in call.pip_requirements)


def test_sagemaker_instance_selection_picks_smallest_that_fits():
    from ds_platform_utils.metaflow.compute_backends import resolve_instance_type

    assert resolve_instance_type(cpu=2) == "ml.m7i.large"
    assert resolve_instance_type(cpu=8, memory=32) == "ml.m7i.2xlarge"
    assert resolve_instance_type(gpu=1) == "ml.g4dn.xlarge"
    assert resolve_instance_type() == "ml.m7i.large"


def test_sagemaker_rejects_a_request_nothing_satisfies():
    from ds_platform_utils.metaflow.compute_backends import resolve_instance_type

    with pytest.raises(RuntimeError, match="No instance type"):
        resolve_instance_type(cpu=512)


def test_sagemaker_backend_describes_a_real_per_job_choice():
    # The point of the comparison: this is chosen per job, where Snowflake selects from pools that
    # must already exist.
    from ds_platform_utils.metaflow.compute_backends import RemoteCall, Resources, SageMakerBackend

    backend = SageMakerBackend(role_arn="arn:role", image_uri="img", s3_prefix="s3://bucket/prefix")
    call = RemoteCall(
        step_name="train",
        fn_bytes=b"",
        inputs={},
        write_names=[],
        step_names=[],
        python_version=(3, 11),
        resources=Resources(cpu=8, memory=32),
    )
    assert "ml.m7i.2xlarge" in backend.describe(call)


# ---------------------------------------------------------------------------
# Flow packages -- a step inherits what the flow already declares
# ---------------------------------------------------------------------------


class _FakePypiDecorator:
    """Stands in for @pypi_base / @pypi, which the decorator reads by name and attributes."""

    def __init__(self, name, packages):
        self.name = name
        self.attributes = {"packages": packages}


def test_step_inherits_the_flows_pypi_base_packages():
    # The whole point: a flow declares its dependencies once, and a body imports the same names
    # wherever it runs. Without this the container is a second, invisible dependency list.
    from ds_platform_utils.metaflow.external_compute import remote_step

    backend = _FakeBackend()

    @remote_step(backend)
    def step_body(self):
        self.out = 1
        self.next(self.end)

    flow = _FakeFlow()
    flow._flow_decorators = {"pypi_base": [_FakePypiDecorator("pypi_base", {"xgboost": "", "pandas": "2.2.3"})]}
    step_body(flow)

    assert sorted(backend.calls[0].pip_requirements) == ["pandas==2.2.3", "xgboost"]


def test_step_level_pypi_overrides_the_flow_base():
    # Matches Metaflow's own precedence, so the decorator does not invent a third rule.
    from ds_platform_utils.metaflow.external_compute import remote_step

    backend = _FakeBackend()

    @remote_step(backend)
    def step_body(self):
        self.out = 1
        self.next(self.end)

    flow = _FakeFlow()
    flow._flow_decorators = {"pypi_base": [_FakePypiDecorator("pypi_base", {"xgboost": "1.0.0"})]}
    type(flow).step_body.decorators = [_FakePypiDecorator("pypi", {"xgboost": "2.0.3"})]
    try:
        step_body(flow)
    finally:
        del type(flow).step_body.decorators

    assert backend.calls[0].pip_requirements == ["xgboost==2.0.3"]


def test_explicit_packages_win_over_inherited_ones():
    from ds_platform_utils.metaflow.external_compute import remote_step

    backend = _FakeBackend()

    @remote_step(backend, packages={"xgboost": "9.9.9"})
    def step_body(self):
        self.out = 1
        self.next(self.end)

    flow = _FakeFlow()
    flow._flow_decorators = {"pypi_base": [_FakePypiDecorator("pypi_base", {"xgboost": "1.0.0"})]}
    step_body(flow)

    assert backend.calls[0].pip_requirements == ["xgboost==9.9.9"]


def test_inheritance_can_be_switched_off():
    # For a flow whose submitting side needs packages the body does not, where installing them
    # in a container is pure cost.
    from ds_platform_utils.metaflow.external_compute import remote_step

    backend = _FakeBackend()

    @remote_step(backend, inherit_flow_packages=False)
    def step_body(self):
        self.out = 1
        self.next(self.end)

    flow = _FakeFlow()
    flow._flow_decorators = {"pypi_base": [_FakePypiDecorator("pypi_base", {"xgboost": ""})]}
    step_body(flow)

    assert backend.calls[0].pip_requirements == []


def test_a_flow_with_no_pypi_base_still_works():
    from ds_platform_utils.metaflow.external_compute import remote_step

    backend = _FakeBackend()

    @remote_step(backend)
    def step_body(self):
        self.out = 1
        self.next(self.end)

    step_body(_FakeFlow())

    assert backend.calls[0].pip_requirements == []


# ---------------------------------------------------------------------------
# Requirement filtering -- the other half of inheriting a whole flow's packages
# ---------------------------------------------------------------------------


def test_requirement_name_handles_every_shape_that_reaches_it():
    from ds_platform_utils.metaflow.compute_backends import requirement_name

    assert requirement_name("xgboost") == "xgboost"
    assert requirement_name("pandas==2.2.3") == "pandas"
    assert requirement_name("scikit-learn>=1.3") == "scikit-learn"
    assert requirement_name("snowflake_ml_python") == "snowflake-ml-python"
    assert requirement_name("git+https://github.com/patterninc/ds-platform-utils.git@main") == "ds-platform-utils"
    assert requirement_name("ds-platform-utils @ git+https://github.com/x/ds-platform-utils.git@main") == (
        "ds-platform-utils"
    )


def test_available_packages_are_skipped_only_when_unversioned():
    # A pin is a statement of intent. Letting pip confirm an already-satisfied pin costs a second;
    # silently running against a different version costs a debugging session.
    from ds_platform_utils.metaflow.compute_backends import filter_preinstalled

    available = frozenset({"pandas", "xgboost"})
    assert filter_preinstalled(["xgboost"], available=available) == []
    assert filter_preinstalled(["pandas==2.2.3"], available=available) == ["pandas==2.2.3"]
    assert filter_preinstalled(["shap"], available=available) == ["shap"]


def test_vcs_requirements_in_the_image_are_always_skipped():
    # Not an optimisation: the image purges git, so git+https:// could not install at all.
    from ds_platform_utils.metaflow.compute_backends import filter_preinstalled

    requirements = ["git+https://github.com/patterninc/ds-platform-utils.git@main"]
    assert filter_preinstalled(requirements, available=frozenset({"ds-platform-utils"})) == []
    assert filter_preinstalled(requirements, available=frozenset()) == requirements


def test_never_install_wins_over_any_pin():
    # `never_install` exists for packages that would break the container rather than merely cost
    # time, so a pin must not rescue them the way it does for `available`.
    from ds_platform_utils.metaflow.compute_backends import filter_preinstalled

    requirements = ["awscli==1.2.3", "boto3", "xgboost"]
    assert filter_preinstalled(requirements, never_install=frozenset({"awscli", "boto3"})) == ["xgboost"]


def test_image_packages_match_the_dockerfile():
    # These two drift silently: a name here that the image lacks becomes a ModuleNotFoundError in
    # a container nobody can see. Cheap to check, so check it.
    import re
    from pathlib import Path

    from ds_platform_utils.metaflow.compute_backends import IMAGE_PACKAGES, requirement_name

    dockerfile = (Path(__file__).parents[2] / "docker/Dockerfile").read_text()
    installed = {
        requirement_name(line)
        for line in re.findall(r"^\s+([A-Za-z0-9_.\-]+(?:==[^\s\\]+)?|\"[^\"]+\")\s*\\?$", dockerfile, re.MULTILINE)
    }
    assert IMAGE_PACKAGES <= installed, f"in IMAGE_PACKAGES but not the image: {sorted(IMAGE_PACKAGES - installed)}"


def test_selectable_instances_are_warm_pool_capable():
    # The m5 family has a warm-pool quota of 0 in every account checked, so `keep_alive_seconds`
    # on m5 is silently ineffective -- the job pays the full cold start regardless. Selecting from
    # families that *can* hold a warm pool is what makes that option real rather than decorative.
    from ds_platform_utils.metaflow.compute_backends import SAGEMAKER_INSTANCE_TYPES

    cpu_only = {name for name, spec in SAGEMAKER_INSTANCE_TYPES.items() if spec["gpu"] == 0}
    assert cpu_only, "expected some CPU instance types"
    assert not [name for name in cpu_only if name.startswith("ml.m5.")], (
        "m5 has no warm-pool quota; use m7i/c7i/r7i for CPU steps"
    )


# ---------------------------------------------------------------------------
# Cancellation -- a job nobody is waiting for still bills
# ---------------------------------------------------------------------------


class _CancellableJob:
    """A handle that records whether it was asked to stop."""

    id = "job-1"
    status = "InProgress"
    terminal_statuses = frozenset({"Completed", "Failed", "Stopped"})

    def __init__(self, error=None):
        self.cancelled = False
        self._error = error

    def get_logs(self):
        if self._error:
            raise self._error
        return ""

    def result(self):
        return b"done"

    def cancel(self):
        self.cancelled = True


def test_interrupt_while_waiting_stops_the_job():
    # The job does not know nobody is listening. Without this it runs to its stopping condition,
    # billing the whole time, with no reference left anywhere to stop it.
    from ds_platform_utils.metaflow.external_compute import await_job_or_cancel

    job = _CancellableJob(error=KeyboardInterrupt())

    with pytest.raises(KeyboardInterrupt):
        await_job_or_cancel(job, label="train")

    assert job.cancelled, "an interrupted wait must stop the job"


def test_a_failing_cancel_does_not_mask_the_original_error():
    from ds_platform_utils.metaflow.external_compute import await_job_or_cancel

    class _StubbornJob(_CancellableJob):
        def cancel(self):
            raise RuntimeError("stop call failed")

    # The interrupt is what the caller needs to see, not a secondary cleanup failure.
    with pytest.raises(KeyboardInterrupt):
        await_job_or_cancel(_StubbornJob(error=KeyboardInterrupt()), label="train")


def test_a_normal_completion_does_not_cancel():
    from ds_platform_utils.metaflow.external_compute import await_job_or_cancel

    job = _CancellableJob()
    job.status = "Completed"

    assert await_job_or_cancel(job, label="train") == b"done"
    assert not job.cancelled


def test_runtime_cap_is_generous_enough_for_real_training():
    # This decorator exists for the heavy steps, and a real training step can run over an hour.
    # A cap that kills legitimate work breaks the flow every time; one that lets a stranded job
    # run costs money occasionally. Handles cancel themselves, so this only backstops SIGKILL.
    from ds_platform_utils.metaflow.compute_backends import DEFAULT_MAX_RUNTIME_SECONDS, SageMakerBackend

    assert DEFAULT_MAX_RUNTIME_SECONDS >= 24 * 60 * 60
    backend = SageMakerBackend(role_arn="arn:role", image_uri="img", s3_prefix="s3://bucket/prefix")
    assert backend.max_runtime_seconds == DEFAULT_MAX_RUNTIME_SECONDS

    # ...but a flow that knows its duration can tighten it.
    assert (
        SageMakerBackend(
            role_arn="arn:role", image_uri="img", s3_prefix="s3://bucket/prefix", max_runtime_seconds=900
        ).max_runtime_seconds
        == 900
    )


def test_a_failed_body_is_not_cancelled_again():
    # The body raising means SageMaker already ended the job and marked it Failed. Asking to stop
    # it then reports an error about a job that did exactly what it should, printed on top of the
    # traceback the user actually needs to read.
    from ds_platform_utils.metaflow.external_compute import await_job_or_cancel

    job = _CancellableJob(error=None)
    job.status = "Failed"
    job.result = lambda: (_ for _ in ()).throw(RuntimeError("body blew up"))

    with pytest.raises(RuntimeError, match="body blew up"):
        await_job_or_cancel(job, label="train")

    assert not job.cancelled, "a job AWS already ended must not be stopped again"


def test_unreadable_status_is_treated_as_still_running():
    # `_already_finished` decides whether to bother stopping a job. An unknown state must read as
    # "still running": a needless stop call is cheaper than a stranded job billing for hours.
    from ds_platform_utils.metaflow.external_compute import _already_finished

    terminal = frozenset({"Completed", "Failed", "Stopped"})

    class _Opaque:
        terminal_statuses = terminal

        @property
        def status(self):
            raise ConnectionError("cannot reach AWS")

    class _Done:
        terminal_statuses = terminal
        status = "Completed"

    class _Running:
        terminal_statuses = terminal
        status = "InProgress"

    assert _already_finished(_Done()) is True
    assert _already_finished(_Running()) is False
    assert _already_finished(_Opaque()) is False


def test_failure_message_carries_the_container_traceback():
    # SageMaker's FailureReason for a crashed body is "AlgorithmError: , exit code: 1" -- true and
    # useless. The cause is in the container's output, and that is what a reader needs first.
    from ds_platform_utils.metaflow.compute_backends import _failure_message

    logs = "starting\n" + "\n".join(f"line {i}" for i in range(50)) + "\nValueError: boom\n"
    message = _failure_message("train-1", "AlgorithmError: , exit code: 1", logs)

    assert "train-1" in message
    assert "AlgorithmError" in message, "AWS's own reason is still worth keeping"
    assert "ValueError: boom" in message, "the actual cause must be in the message"
    assert "starting" not in message, "only the tail, not the whole log"


def test_failure_message_survives_having_no_logs():
    # A job that died before writing anything still has to raise something readable.
    from ds_platform_utils.metaflow.compute_backends import _failure_message

    assert "no reason given" in _failure_message("train-1", None, "")
    assert "container output" not in _failure_message("train-1", "InternalServerError", "")


# ---------------------------------------------------------------------------
# Writes that never happened
# ---------------------------------------------------------------------------


def test_conditional_writes_are_told_apart_from_unconditional_ones():
    # Reporting both the same way makes the warning noisy on any flow that branches, and a noisy
    # warning is an ignored one.
    from ds_platform_utils.metaflow.external_compute import analyze_self_access, conditional_writes

    def body(self):
        self.always = 1
        if self.flag:
            self.only_if = 2
        for item in self.items:
            self.in_loop = item
        try:
            self.in_try = 3
        except ValueError:
            self.in_handler = 4

    _, writes = analyze_self_access(body)
    branch = conditional_writes(body)

    assert set(writes) - branch == {"always"}
    assert branch == {"only_if", "in_loop", "in_try", "in_handler"}


def test_a_branch_test_is_not_itself_conditional():
    # `if self.flag:` evaluates unconditionally, so a write in the test expression is not
    # branch-dependent -- only the body is.
    from ds_platform_utils.metaflow.external_compute import conditional_writes

    def body(self):
        if self.flag:
            self.inner = 1

    assert conditional_writes(body) == {"inner"}


def test_missing_writes_are_reported_not_raised():
    # A branch that did not run is ordinary Python; the step must still succeed.
    import cloudpickle

    from ds_platform_utils.metaflow.external_compute import execute_remote_step

    def body(self):
        self.always = 1
        if False:
            self.never = 2
        self.next(self.end)

    blob = execute_remote_step(
        cloudpickle.dumps(body), {}, ["always", "never"], ["end"], b"", {}, sys.version_info[:2]
    )
    result = cloudpickle.loads(blob)

    assert "always" in result["writes"]
    assert result["missing_writes"] == ["never"]


def test_batch_logs_do_not_mask_a_failure_reason():
    # A job that dies before its container starts has a stream name but no stream. get_logs() is
    # called from the failure path, so raising there would replace the reason the job failed with
    # a complaint about missing logs.
    from ds_platform_utils.metaflow.compute_backends import BatchJobHandle

    class _NoStream:
        def get_log_events(self, **_kwargs):
            raise RuntimeError("The specified log stream does not exist.")

    class _Session:
        def client(self, name):
            return _NoStream() if name == "logs" else object()

    handle = BatchJobHandle("id-1", "job-1", "s3://b/k", _Session())
    handle._log_stream = "remote-step/default/abc"

    assert handle.get_logs() == ""


def test_fargate_sizes_round_up_and_refuse_the_impossible():
    # Fargate accepts only certain vCPU/memory pairings and rejects anything else at submit time
    # rather than rounding, so the choice has to be made before the job is sent.
    from ds_platform_utils.metaflow.compute_backends import resolve_fargate_size

    assert resolve_fargate_size(cpu=1, memory=2) == ("1", "2048")
    assert resolve_fargate_size(cpu=2, memory=4) == ("2", "4096")
    # asking for more memory than that vCPU allows moves up a size rather than failing
    assert resolve_fargate_size(cpu=1, memory=16) == ("8", "16384")

    with pytest.raises(RuntimeError, match="EC2 compute environment"):
        resolve_fargate_size(cpu=48, memory=192)
