# `@remote_step` scenario flows

Real flows, run against the real cluster. The unit tests under
`tests/unit_tests/remote_step/` check the pieces; these check what a user
actually writes, through both orchestrators.

That distinction has earned its keep. Two defects reached this suite that 448
unit tests could not see, because the tests stubbed exactly the thing that was
broken:

- `RemoteArtifact` did not proxy comparison, arithmetic or string formatting,
  so in a downstream plain step `self.result == 499500` was **False** and
  `f"{self.total}"` printed the ref's repr. Four flows failed on it
  independently.
- Merging a join's artifacts by pointer dropped `read_role_arn`, so a
  downstream plain step hit `AccessDenied` as `obp-5p6le9-task` — one step
  after the cause. Every unit test around that path stubs S3.

## Running

The project is pinned to a git rev of `ds-platform-utils`, exactly as a
consumer project is, so the *driver* code under test is the pinned one and the
*runner* code comes from the image. Re-pin after pushing a change:

```bash
# point pyproject.toml's rev at the commit you want, then
uv sync
```

```bash
bash run_matrix.sh   local            # metaflow `run`, driver in-process
bash run_matrix.sh   argo             # deploy + trigger, driver on an Argo pod
bash run_matrix.sh   local fx02_foreach.py fx15_wide_fanout.py
bash run_parallel.sh local fx0*.py    # one process per flow
```

Warm the wheel cache with one sequential run before fanning out — the
micromamba cache is global and racing it from cold corrupts it.

Each flow prints `[check] PASS/FAIL <label>` per assertion and raises on the
first failure, so a broken run fails the step rather than logging something
nobody reads.

## What each flow covers

| flow | covers |
|---|---|
| `fx01_basic` | linear step, `@resources`, arm64 default, the resource-usage log line |
| `fx02_foreach` | 3-way foreach + join, branch attrs **over** the 4 MB inline limit, `self.index`, `foreach_stack()` |
| `fx03_outputs` | new / reassigned / `None` / in-place-mutated / freed-then-rebuilt attributes |
| `fx04_timeout` | `@timeout(seconds=...)`, whose body outlives the old 60 s deadline |
| `fx05_switch` | `self.next({...}, condition=...)` on a remotely-produced condition |
| `fx06_decorators` | `@card` above `@remote_step` with `@kubernetes` present; `disk`, `shared_memory` |
| `fx07_exception` | `@catch` over picklable and unpicklable exceptions |
| `fx08_merge` | static split, `merge_artifacts`, large artifacts read in the join |
| `fx09_merge_unread` | `merge_artifacts` where the join never reads them (pointer path) |
| `fx10_sandbox` | no team tag → the `sandbox` namespace fallback |
| `fx11_project` | `@project` context: `is_production`, `branch_name` |
| `fx12_retry` | `@retry`, `current.retry_count` |
| `fx13_gpu` | GPU ask → x86 fallback, `nvidia-smi`, `@gpu_profile` past its 300 s boundary |
| `fx14_secrets` | `@secrets` reaching the pod (names only, never values) |
| `fx15_wide_fanout` | 40-way foreach, distinct large payload per branch |
| `fx16_nested_foreach` | foreach inside foreach, joined at both levels |
| `fx17_pypi_step` | `@pypi` on the step |
| `fx18_pypi_base` | `@pypi_base` on the flow |
| `fx19_uv_pypi_base` | `@uv_pypi_base` from `uv.lock`, **including the private git dependency** |
| `fx20_uv_pypi_group` | `@uv_pypi(dependency_groups=...)`, and that an unnamed group is absent |
| `fx21_params` | `Parameter` of four types, `IncludeFile` |
| `fx22_context` | `@environment`, `current.run.add_tags` from the pod, Metaflow client in the pod |
| `fx23_s3_integration` | assuming an Outerbounds S3 integration role from the pod |
| `fx24_sizing_refusals` | asks that must be **refused**, and large ones that must not be |
| `fx25_mixed_chain` | remote → plain → remote, many outputs at once |
| `fx26_deferred_inputs` | reading one of several large inputs; the rest must never download |
| `fx27_config` | production shape: pydantic `Config`, `@project`, `@schedule`, `@pypi_base` |
| `fx28_conda_refused` | `@conda` with packages refused; a bare `conda` **not** refused |
| `fx29_mutator_and_hooks` | a custom `FlowMutator` + `user_step_decorator` around a remote step |

`fx10` deliberately carries no team tag; the runners know to omit it.

## Coverage came from the consumer repos

The decorator list above was checked against every flow in
`data-science-projects`, `demand-forecast`, `sales-suite-backend`,
`heimdall-sdk` and `outerbounds-snowpark` — 107 files — by tallying what they
actually use. `@trigger_on_finish` (36 uses) and `@schedule` (34) are
deploy-time concerns covered by deploying `fx27`; `@snowflake_step` and
`@snowpark` are separate compute backends and are not combined with
`@remote_step`.
