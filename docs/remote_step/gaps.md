# `@remote_step` gaps — what is not implemented yet

Consolidated list of every known limitation, missing feature, or footgun in the
`@remote_step` decorator, ordered roughly by production impact. This is the
punch list for making the decorator "rock solid" for real workloads.

Numbers in the **Uses** column come from an audit of the two production repos
`data-science-projects` (83 flows, 566 steps) and `pattern-nlp` (87 flows,
565 steps) as of 2026-09-03.

Legend for **Status**:
- ✅ shipped, verified end-to-end
- ⚠️ shipped, partial / untested
- 🚧 known broken, fix designed but not shipped
- ❌ not started
- 🚫 explicit refusal by design

> **Terminology predates the EKS migration.** Most entries below say "Batch",
> "Batch job" or "Batch container" — the decorator now submits a Kubernetes Job
> to our own EKS cluster (`pattern-ml-platform`), queued through Kueue and
> placed by Karpenter. Read "Batch" as "the runner pod". The *findings* still
> hold, because they are all about the split between the thin driver and the
> remote container, which the migration did not change; only the proposed
> fixes that name Batch-specific machinery (job queues, for instance) need
> rethinking. Not yet reworded, to avoid churning 400 lines of accurate
> analysis for a naming pass.
>
> See also [security_review.md](security_review.md) for the auth chain and its
> findings.

---

## Blockers — real flows fail today

### 1. `def join(self, inputs)` — join step signature — 🚧
- **Uses**: 58 join steps across the two repos.
- **Bug**: driver body calls `original(fake)` but a join step's user body is
  `def join(self, inputs)`. Missing positional argument → `TypeError`.
- **Fix**: detect `node.type == "join"` at step_init and pass a synthetic
  `inputs` object (Metaflow's `Inputs` shape) to `original(fake, inputs)`.
- **Files**: `metaflow_extensions/remote_step/runner_entry.py`.

### 2. `self.merge_artifacts(inputs, include=[...])` — 🚧
- **Uses**: 26 join sites.
- **Bug**: on Batch, `_FakeSelf.__getattr__` returns a no-op placeholder for
  `.merge_artifacts`, so upstream artifacts are silently dropped.
- **Fix**: implement `merge_artifacts` on `_FakeSelf` — copy each attribute
  from every `inputs` entry onto `self`, honouring `include=` / `exclude=`.

### 3. `self.input` inside foreach child steps — 🚧
- **Uses**: ~100 sites in foreach branches (`self.worker = self.input`,
  `Run(pathspec=self.input)`, tuple-unpack `self.month, self.country = self.input`).
- **Bug**: `_FakeSelf.input` is hardcoded to `None`.
- **Fix**: driver passes the foreach split value into spec; runner_entry sets
  `fake.input = spec.get("foreach_input")`; `_collect_flow_attrs` reads it
  from the current flow's `self.input` property before shipping.

### 4. `current.is_production` — 🚧
- **Uses**: 209 sites — dominant `current.*` attribute across both repos.
- **Bug**: not patched on Batch → evaluates falsy → user code writes to STAGE
  tables from a PROD run. Silent, causes wrong-schema production writes.
- **Fix**: forward `is_production` bool from driver's `metaflow.current` into
  spec, patch `_current._is_production` in runner_entry.

### 5. `current.branch_name` / `current.project_name` — 🚧
- **Uses**: 4 sites (branch_name) + implicit uses via `Flow(...).runs('project_branch:prod')`.
- **Bug**: not patched.
- **Fix**: forward from `metaflow.current` on driver, set in runner_entry.

### 6. `current.card` + `@card(type="html")` — ❌
- **Uses**: 220 `@card`-decorated steps, 66 `current.card.append(...)` sites.
- **Bug**: `@card` runs in Metaflow's `task_finished` on the driver task, which
  sees a stripped-down `self` populated with `RemoteArtifact` refs. User's
  `current.card.append(Markdown(...))` inside the Batch step body writes into
  a Metaflow card sidecar that isn't connected to the driver's card rendering.
  Result: cards render empty for `@remote_step` steps.
- **Options**:
  a. Serialise `card_data` from Batch → driver replays `current.card` calls.
  b. Hydrate specific outputs (marked by user) before card render.
  c. Explicitly refuse `@card` on `@remote_step` and document.
- **Decision needed**: (a) is the "right" answer, (b) is the pragmatic one.

### 7. `current.model` / `@model(load=[...])` — ❌
- **Uses**: 19 sites (embedding models, sklearn, spaCy, `distilbart_mnli_12_3`, etc.).
- **Bug**: `@model` downloads model artifacts on the driver argo pod, populates
  `current.model.loaded[...]`. Batch container has neither the files nor the
  populated dict.
- **Options**:
  a. Ship `current.model.loaded` mapping as part of spec, re-download models
     via boto3 on Batch.
  b. Replay `@model` decorator's `task_pre_step` on Batch (import Metaflow
     model_load plugin server-side).
  c. Refuse `@remote_step` when `@model` is present.

### 8. `current.huggingface_hub` / `@huggingface_hub` — ❌
- **Uses**: 17 sites.
- Same shape as `@model`: decorator runs on driver, Batch container doesn't
  have the HF snapshots.
- **Options**: mirror decisions for `@model`.

### 9. Metaflow client inside step body — ❌
- **Uses**: 30+ sites of `Flow(...).latest_successful_run`, `Run(pathspec=...)`,
  `Task(...)`, `namespace(...)`, `default_namespace()`.
- **Bug**: Batch container has mfconfig env vars but likely lacks IAM permission
  on Outerbounds' Metaflow datastore S3 bucket. Cross-account read via our
  bucket policy doesn't help — OB's datastore is in a different account.
- **Fix**: add `sts:AssumeRole` on a cross-account read role Outerbounds
  provides, OR verify that mfconfig-based auth uses our submit-user credentials
  (which have S3 access to some but maybe not all Metaflow datastores).

### 10. `current.run.add_tags(...)` — ❌
- **Uses**: 7 sites.
- **Bug**: `current.run` needs a live Metaflow `Run` client on Batch; not wired.
- **Fix**: build a `Run` client in runner_entry using forwarded mfconfig, patch
  `current._run`.

---

## Major functional gaps — degrade UX / semantics

### 11. `@environment(vars={...})` — 🚧
- **Uses**: 1 site (rare but real).
- **Bug**: env vars set on argo pod, not forwarded to Batch. Only allow-listed
  prefixes (`METAFLOW_*, OBP_*, OUTERBOUNDS_*, GITHUB_TOKEN`) reach Batch.
- **Fix**: read sibling `@environment` decorator's attrs, merge into
  `containerOverrides.environment` in `submit.py`.

### 12. `@catch(var="e")` preserving original exception — 🚧
- **Uses**: 6 sites.
- **Bug**: user's step body exception on Batch bubbles as `RunnerError` on the
  driver. `@catch` captures `RunnerError`, not the user's original exception.
- **Fix**: pickle the original exception into a spec output field on Batch,
  driver re-raises exactly that.

### 13. `@timeout` sync driver ↔ Batch — 🚧
- **Uses**: 126 sites.
- **Bug**: user's `@timeout(minutes=N)` applies to the driver argo pod. Batch
  job has its own `job_timeout_minutes` attr on `@remote_step`. If argo pod
  timeout fires while Batch is still running, driver is killed but Batch keeps
  billing.
- **Fix**: use user's `@timeout` value as the Batch job timeout AND the driver
  pod timeout (with a small pad so driver outlives Batch on the timeout race).

### 14. `@gpu_profile()` — ❌
- **Uses**: 8 sites (advertising CR flows).
- **Bug**: `@gpu_profile` decorator runs on driver (argo pod), samples the
  driver's GPU (there is none). No sampling happens on Batch.
- **Fix**: shift `@gpu_profile` onto the Batch step. Requires the profiler to
  work inside our runner_entry.

### 15. `compute_pool` argument to `@kubernetes` — ✅
- **Uses**: 10 sites (`g6e-4xlarge-nlp`, `c8a-8xlarge-content`).
- **Behaviour**: the pool places the **driver** pod, not the step body. A
  sibling `@kubernetes` is removed so it cannot size the driver to the step's
  full ask, but its placement attributes — `compute_pool`, `node_selector`,
  `namespace`, `tolerations` — are carried onto the driver's own `@kubernetes`
  while cpu/memory/gpu are forced to driver size (2 vCPU / 8 GB). A step
  asking for a 29 GB pool gets its driver on that pool at driver size, not a
  29 GB pod holding a poll loop. Reported at flow init.
- `cpu` / `memory` / `gpu` on that same decorator are now read as the step's
  resource ask. Previously only `@resources` was read, so a step declaring its
  size solely on `@kubernetes` silently got 1 vCPU / 4 GB.
- The step body always runs on our EKS cluster, where Karpenter selects the
  instance — Outerbounds pool names have no meaning there.

### 16. `@conda` / `@conda_base` — ❌
- **Uses**: 2 sites (promo-lift flows).
- **Bug**: our env resolver reads `@pypi`/`@pypi_base` only. Conda envs won't
  be respected.
- **Fix option**: reject `@conda` on `@remote_step` with a clear message, or
  translate to `@pypi` where possible.

---

## Historical footguns already fixed

### F1. Metaflow extension not discovered — ✅
- **Cause**: hatchling doesn't emit `top_level.txt`.
- **Fix**: switched `ds-platform-utils` to setuptools build backend.

### F2. Python 3.9 union-type syntax error on argo pod — ✅
- **Cause**: `X | None` unsupported in Py 3.9.
- **Fix**: `from __future__ import annotations` on every module.

### F3. `remote_step` module not found in Batch container — ✅
- **Fix**: consolidated runtime under `metaflow_extensions/remote_step/`, added
  `sys.modules["remote_step"] = _this` alias in `__init__.py`.

### F4. Batch job name rejected (Argo run-id contains dots) — ✅
- **Fix**: sanitize with `re.sub(r"[^A-Za-z0-9_-]", "-", raw)[:128]`.

### F5. Cross-account 403 on Outerbounds S3 code-package — ✅
- **Fix**: driver tars local `.mf_code` and uploads to our payload bucket
  instead of forwarding Outerbounds' datastore URL.

### F6. Spec env packages empty — Metaflow blanks `@pypi_base` at task-run time — ✅
- **Fix**: write `.remote_step_env.json` alongside flow at argo-create-time,
  ship via `add_to_package`, read on argo pod as fallback.

### F7. `ds-dqv-tool==@ git+...` uv parse error — ✅
- **Fix**: entrypoint constructs `f"{name} {ver}"` for `@ git+...` refs.

### F8. Shell splits `pkg @ url` on spaces — ✅
- **Fix**: null-terminated bash array (`while IFS= read -r -d ''`).

### F9. `fatal: could not read Username for github.com` in Batch — ✅
- **Fix**: forward `GITHUB_TOKEN` via `containerOverrides.environment`,
  entrypoint writes `~/.netrc`.

### F10. `GITHUB_TOKEN` missing in driver pod — ✅
- **Fix**: separate `remote-step-github` Outerbounds custom-secret; decorator
  injects `@secrets(sources=['outerbounds.remote-step-github'])`.

### F11. OBP Snowflake integration URL missing in Batch — ✅
- **Fix**: forward `METAFLOW_*, OBP_*, OUTERBOUNDS_*` env vars from driver
  into `containerOverrides.environment`.

### F12. Slack webhook error "No Slack webhook URL found. Tag your run with…" — ✅
- **Root**: `current.tags` empty in Batch container.
- **Fix**: forward `current.tags` + `current.system_tags` via spec, patch
  `_current._tags` / `_system_tags` in `runner_entry`.

### F13. `df_core_raw must be a pandas DataFrame` — ✅
- **Root**: runner_entry didn't materialise `RemoteArtifact` inputs into
  native Python objects.
- **Fix**: `_hydrate_input` calls `.load()` on RemoteArtifact-kind spec
  entries.

### F14. All logs in stderr, not stdout — ✅
- **Fix**: replaced every `sys.stderr.write` with `sys.stdout.write` across
  submit / decorator / poll / runner_entry.

### F15. Driver logs not realtime — ✅ mostly
- **Fixes**:
  - `sys.stdout.reconfigure(line_buffering=True)` on driver start.
  - `logs:StartLiveTail` streams CW events from driver to stdout with
    sub-second latency (bypassing the previous 15 s poll loop).
  - Force-run `metaflow.mflog.save_logs` every 3 s from a driver thread so
    Outerbounds UI polling picks up mid-task updates.
- **Residual**: Fargate awslogs driver buffers ~5 s before emitting to CW —
  hard AWS floor we can't beat without swapping log driver.

### F16. CloudWatch console URL — `logGroupName` regex error — ✅
- **Fix**: double-URL-encode with `$252F` (`quote(quote(s, safe=''),
  safe='').replace('%', '$')`) in `poll._cw_console_url`.

### F17. Metaflow blanks `@pypi_base.packages` at task-run time — ✅
- **Fix**: `.remote_step_env.json` bundling via `add_to_package`.

### F18. `RemoteArtifact` not subscriptable in downstream non-remote step — ✅
- **Root**: downstream `build_df_inquiry` did `self.df_prep_weekly[cols]`;
  attribute was a `RemoteArtifact` ref, not a DataFrame.
- **Fix**: transparent-proxy dunders on `RemoteArtifact` (`__getitem__`,
  `__getattr__`, `__iter__`, `__len__`, `__contains__`, `__bool__`) that
  lazy-load on first non-@remote_step access.

### F19. `RemoteArtifact.__call__` regression — ✅
- **Root**: added `__call__` in the proxy dunders made `callable(ref)` return
  True → driver's `_collect_flow_attrs` callable filter silently dropped
  every upstream ref → runner got no inputs → `_FakeSelf.__getattr__` no-op
  placeholder → DQV saw callable, not DataFrame.
- **Fix**: dropped `__call__`; hardened `_try_add` to whitelist
  `RemoteArtifact` past the callable check.

### F20. Cross-account S3 read from non-@remote_step downstream — ✅
- **Root**: Outerbounds pod IAM has a permissions boundary that denies
  cross-account `s3:GetObject`. Bucket policy alone can't override it.
- **Fix**: created `remote-step-dev-ob-artifact-reader` IAM role in our
  account with a trust policy for Outerbounds' pod task role, tagged
  `outerbounds.com/accessible-by-deployment=pattern`, S3 read on payload
  bucket. `RemoteArtifact.load()` tries direct S3 first, falls back to
  `sts:AssumeRole` on any `ClientError`/`BotoCoreError`. STS sessions cached
  per role ARN, refreshed at 45 min.

### F21. `libgomp.so.1: cannot open shared object file` — ✅
- **Root**: LightGBM's C extension needs OpenMP runtime. `python:3.10-slim`
  doesn't ship `libgomp1`.
- **Fix**: `apt-get install libgomp1` in Dockerfile.

### F22. Broader ML-lib runtime coverage — ✅
- **Fix**: preinstalled `libgomp1 libgfortran5 libstdc++6 libgcc-s1 libblas3
  liblapack3 libopenblas0-pthread libgl1 libglib2.0-0 libsm6 libxext6
  libxrender1 libjpeg62-turbo libpng16-16 libfreetype6 libtiff6 libwebp7
  zlib1g libhdf5-103 libpq5 libgeos-c1v5 libproj25 librdkafka1 libssl3
  libkrb5-3`. Adds ~40 MB. GPU workloads deferred to a separate CUDA image.

### F23. Switch step ran both branches — ✅
- **Root**: driver replayed `self.next(*out_funcs)` for every step type,
  turning `self.next({True: X, False: Y}, condition="run_dqv")` into a
  parallel split.
- **Fix**: capture `node.type`, `switch_cases`, `condition`, `foreach_param`,
  `num_parallel` at step_init; rebuild the correct `self.next(...)` call at
  driver end.

---

## Execution modes

### E1. `run --with kubernetes` — ✅

The driver runs on an Outerbounds pod at Small tier and the step body on
EKS, matching the Argo path. Previously the `--with kubernetes` decorator
was silently removed and the driver stayed on the laptop; see finding 8 in
[security_review.md](security_review.md).

Remaining differences from Argo, none of them regressions:

1. **`current.is_production` is False.** The developer's `current.namespace`
   is not `prod`. Code branching on it writes to STAGE tables. Applies to
   every local mode, not this one specifically.
2. **Non-`@remote_step` siblings still hit AssumeRole.** They run on OB pods
   and `RemoteArtifact.load()` assumes `ob-artifact-reader` — same as Argo.

### E2. `run` — ✅

Driver in the local Python process, step body still on EKS. Consequences:

1. **No AWS credentials needed.** The driver prefers ambient credentials,
   and falls back to the ones Outerbounds already vends for this perimeter's
   task role — the same identity Argo runs as, valid 24 hours, refreshed by
   botocore's web-identity provider. `aws sso login` is not required, and
   neither is the AWS CLI.
2. **Log streaming differs.** No pod means no `save_logs_periodically`
   sidecar, so driver stdout goes straight to the terminal and the
   Outerbounds UI only shows it after the task completes.
3. **`current.is_production` is False**, as in E1.

### E3. `--with remote_step:team=<team>` — ✅

Offloads every step without decorating any of them, the same way
`--with kubernetes` works. Combines with the modes above: driver local on a
plain `run`, on an Outerbounds pod with `--with kubernetes`, on the Argo pod
under `argo-workflows create`.

`start` and `end` are skipped. Written by hand on those two `@remote_step` is
still refused (R3) — the scheduler owns them — but a sweep cannot avoid
touching them, so refusing would make the flag unusable. The two cases are
told apart by looking for a `--with` spec naming `remote_step` in `sys.argv`.

Each step keeps its own `@resources`, so this is not one blanket size.

`team` may come from `--tag ds.domain:<team>` instead of the decorator, in
which case the flow stops being self-contained — and since an untagged run
falls back to `sandbox` rather than failing, a flow that loses its tag runs on
the wrong quota instead of stopping. The fallback is announced on stderr and
sandbox is too small for a real flow, so it surfaces as a stalled or cramped
run rather than a clear error.

### E4. `argo-workflows create + trigger` (production) — ✅

`_is_argo_context()` is True, so the driver gets a Small-tier `@kubernetes`
plus `@secrets` injection and runs on the Argo pod. Credentials come from
the pod's OIDC task role. This is the mode production flows use; everything
above is developer convenience.


## By design — will not implement

### R1. `@batch` on the same step — 🚫
- Mutually exclusive with `@remote_step` (both offload compute). Explicit
  refusal in step_init with a clear error.

### R2. `@parallel` on the same step — 🚫
- Metaflow parallel (jobset semantics) not implemented. Explicit refusal.

### R3. `@remote_step` written by hand on `start` or `end` — 🚫
- Metaflow's scheduler owns those steps; @remote_step on them would offload
  the wrong compute. Explicit refusal.
- `--with remote_step:team=<team>` is the exception: a sweep cannot avoid
  touching them, so those two are skipped silently rather than failing the
  whole flow. Every other step is offloaded.

### R4. GPU workloads — 🚫 (for now)
- CUDA / cuDNN / NVIDIA drivers deferred to a separate `remote-step-runner-gpu`
  image, not the default runner. Non-goal for the CPU decorator MVP.

---

## Coverage-status snapshot

Broken down by transition / decorator, counted across both production repos.

| Feature | Uses | Status |
|---|---:|---|
| Linear `self.next(a)` | ~925 | ✅ |
| Split `self.next(a, b)` | 5 | ✅ |
| Split-switch `self.next({..}, condition=...)` | 16 | ✅ (F23) |
| Foreach `self.next(a, foreach="x")` | 43 | ⚠️ replay logic added, `self.input` still `None` |
| Foreach parallel `num_parallel=` | 0 | not needed |
| Join `def join(self, inputs)` | 58 | 🚧 gap #1 |
| `self.merge_artifacts(inputs)` | 26 | 🚧 gap #2 |
| `@step` | 1131 | ✅ |
| `@resources` | 112 | ✅ |
| `@kubernetes` | 372 | ✅ (dropped + our small kube injected) |
| `@retry` | 404 | ⚠️ retries entire driver |
| `@card` | 220 | ❌ gap #6 |
| `@secrets` | 97 | ✅ |
| `@timeout` | 126 | 🚧 gap #13 |
| `@catch` | 6 | 🚧 gap #12 |
| `@pypi` / `@pypi_base` | 92 | ✅ |
| `@environment` | 1 | 🚧 gap #11 |
| `@model` | 19 | ❌ gap #7 |
| `@huggingface_hub` | 17 | ❌ gap #8 |
| `@gpu_profile` | 14 | ❌ gap #15 |
| `@compute_pool` (kwarg on `@kubernetes`) | 10 | ❌ gap #16 |
| `@conda` / `@conda_base` | 2 | ❌ gap #17 |
| `@batch` | 0 | 🚫 refused |
| `@parallel` | 0 | 🚫 refused |
| `current.is_production` | 209 | 🚧 gap #4 |
| `current.card` | 66 | ❌ gap #6 |
| `current.run_id` | 40 | ✅ |
| `current.model` | 18 | ❌ gap #7 |
| `current.huggingface_hub` | 17 | ❌ gap #8 |
| `current.flow_name` | 14 | ✅ |
| `current.run.add_tags(...)` | 7 | ❌ gap #10 |
| `current.branch_name` | 4 | 🚧 gap #5 |
| `current.step_name` | 1 | ✅ |
| `current.pathspec` | 1 | ❌ (derive from run_id/step_name/task_id) |
| `current.namespace` (custom) | 1 | ❌ ProjectFlow-provided |
| `Config` | 152 | ✅ |
| `Parameter` | 105 | ✅ |
| `IncludeFile` | 2 | ⚠️ untested |
| `self.merge_artifacts` | 26 | 🚧 gap #2 |
| `self.input` (foreach) | 100+ | 🚧 gap #3 |
| `Flow(...)`, `Run(...)`, `Task(...)` inside step body | 30+ | ❌ gap #9 |
| `parallel_map` inside step body | 17 | ✅ (works if metaflow installed) |
| `config_expr(...)` at decorator-arg time | 1 | ✅ (evaluated at flow init) |
