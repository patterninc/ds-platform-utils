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

### 1. `def join(self, inputs)` — join step signature — ✅
- **Uses**: 58 join steps across the two repos.
- **Was**: the runner called `original(fake)`, so every join died on a missing
  positional argument.
- **Now**: the driver collects each incoming branch's attributes into the spec
  and the runner rebuilds Metaflow's `Inputs` shape, so all three documented
  access patterns work — `inputs.step_a.x`, `inputs[0].x`, and
  `(inp.x for inp in inputs)`.
- Branches are **lazy**: an attribute is fetched on first read and cached, so a
  join over a wide foreach does not pull every branch's data to answer one
  attribute. Branch artifacts that are already `RemoteArtifact` refs stay refs.
- Verified live: `GapsFlow` run 238584 — `gather: 3 branches -> {'us': 100,
  'uk': 50, 'de': 75}`, `inputs[0].region=us`.

### 2. `self.merge_artifacts(inputs, include=[...])` — ✅
- **Uses**: 26 join sites.
- **Was**: `_FakeSelf.__getattr__` answered `.merge_artifacts` with a no-op
  placeholder, so every artifact a join meant to carry forward was dropped in
  silence.
- **Now**: implemented against Metaflow's contract — an attribute already set
  on `self` wins and is skipped; `include` and `exclude` are mutually
  exclusive; an attribute arriving with different content from two branches is
  an unresolved conflict and raises rather than picking one. `include` narrows
  what is considered, it does **not** resolve a conflict (same as Metaflow) —
  assign the attribute yourself, or `exclude` it.
- Conflicts are decided on the content hashes already in the spec, so nothing
  is downloaded to compare.
- Watch out: `hasattr` is useless on the stand-in `self` — `__getattr__`
  answers every non-dunder name — so the "already set" test reads the instance
  dict. Getting that wrong merges nothing at all.
- Verified live: `GapsFlow` run 238584 — `merged run_label='gaps-check'`.

### 3. `self.input` inside foreach child steps — ✅
- **Uses**: ~100 sites in foreach branches (`self.worker = self.input`,
  `Run(pathspec=self.input)`, tuple-unpack `self.month, self.country = self.input`).
- **Was**: `_FakeSelf.input` was hardcoded to `None`, so every foreach child
  saw None — storing nothing, or raising on a tuple unpack.
- **Now**: read from the driver's own `self.input` and shipped in the spec,
  through the same serialisation as any other value (so a foreach splitting on
  something large travels as a ref). `has_foreach_input` distinguishes "not a
  foreach" from "a foreach whose value is legitimately None".
- Verified live: `GapsFlow` run 238584 — `work: region=de rows=75` and one
  branch per region. Before the fix the same flow died on `KeyError: None`.

### 4. `current.is_production` — ✅
- **Uses**: 209 sites — dominant `current.*` attribute across both repos.
- **Was**: unset in the pod, so it read falsy and a PROD run wrote to STAGE
  tables. Silent, no error anywhere.
- **Now**: forwarded from the driver's `current` and replayed in the pod.
  `is_production` is not a built-in property of `current` — @project installs
  it with `_update_env`, which never runs in the runner because the runner is
  not executing a Metaflow task — so the same call is made from the spec.
- Verified live: `ProjFlow` run 238586 with `--production` — driver and pod
  both report `is_production: True`, and the pod logs `would write to PROD`.

### 5. `current.branch_name` / `current.project_name` — ✅
- **Uses**: 4 sites (branch_name) + implicit uses via `Flow(...).runs('project_branch:prod')`.
- **Now**: forwarded with `is_production` — the whole @project set travels:
  `project_name`, `branch_name`, `is_user_branch`, `project_flow_name`.
- Verified live alongside gap 4 (`ProjFlow` run 238586).

### 6. `current.card` + `@card(type="html")` — ✅ (replayed on the driver)
- **Uses**: 220 `@card`-decorated steps, 66 `current.card.append(...)` sites.
- **Was**: `@card` renders on the driver task, so `current.card` did not exist
  in the runner at all — `current.card.append(...)` raised there, and a step
  that guarded the call rendered an empty card either way.
- **Now (option a)**: the pod gets a recorder in place of `current.card` that
  captures what the body appends, and the driver replays it into the real card
  before `@card` renders. `card[id]` is kept, so `@gpu_profile`'s own
  `gpu_profile` card id is carried too.
- Verified live on `CardFlow` run 238599:

  ```
  [remote_step] card.refresh() does nothing in a remote step ...   <- recorder live in the pod
  [remote_step] replayed 3 card component(s) from the step          <- driver appended them
  end: 1 card(s)
  ```

  and every appended component is present in `card.get_data()`.
- **Assert on `card.get_data()`, not on `card.get()`.** The rendered HTML is a
  ~1 MB JS bundle with the component data encoded inside it, so substring
  searching it is meaningless in both directions: while testing this it gave a
  false positive on `"1234"` (a coincidental match in the bundle) and a false
  negative on the Markdown text, which together read as "the replay is
  broken" when it was working the whole time.
- Components travel as pickles. Every Metaflow component takes that except
  `Artifact`, which holds a module reference; those become a Markdown note
  saying what could not cross, rather than a silent hole in the card.
- `card.refresh()` is a no-op with one explanatory line: a live refresh cannot
  reach the driver's card while the pod is still running. The card appears when
  the step finishes.
- A step that appends components but has no `@card` is told so, rather than
  losing them silently.
- Card content is saved even when the body raises, so a failed step's
  diagnostics survive.

### 6b. A card that renders a flow attribute — ✅
- `@card(type="html", options={"attribute": "html"})` does not use
  `current.card` at all: the card reads `self.html` itself, at render time, on
  the driver. A remote step's outputs are `RemoteArtifact` refs by design, so
  the card rendered `RemoteArtifact(kind=..., uri=...)` instead of the report.
  Seen on `dqv_step_input` in the forecast flow.
- **Now**: the attribute names a card declares are read off the sibling
  `@card` decorators, and just those outputs are loaded into values on the
  driver. Everything else stays a ref, so zero-copy is unaffected.
- Capped at 64 MB. A report is kilobytes; silently pulling a 10 GB DataFrame
  into a Small-tier driver to render a card would OOM it. Over the cap the ref
  is left in place with a line saying so and suggesting a summary attribute.
- A failed load falls back to the ref rather than failing the step — a card is
  a report.
- **Not covered**: a card on a *downstream, non-remote* step that renders an
  upstream remote step's artifact. That step is not ours to rewrite, so the
  attribute is still a ref there; call `.load()` in that step, or point the
  card at an attribute produced locally.

### 7. `current.model` / `@model(load=[...])` — ✅ load; save still refused
- **Uses**: 19 sites (embedding models, sklearn, spaCy, `distilbart_mnli_12_3`, etc.).
- **Bug**: `@model` downloads model artifacts on the driver argo pod, populates
  `current.model.loaded[...]`. Batch container has neither the files nor the
  populated dict.
- **Now**: the download happens in the pod, not on the driver, and only the
  *names* travel in the spec. That works because the model reference is an
  ordinary flow artifact — `@model` resolves it with `getattr(flow, name)` —
  which the spec already ships as an input, so the pod has everything it needs
  to fetch the model itself.
- `@model` is **dropped from the driver**, the same way `@kubernetes` is.
  Otherwise its `task_pre_step` downloads a multi-GB model onto a Small-tier
  pod with 10 GB of disk that never reads it. Now the file lands next to the
  GPU and never crosses the driver.
- The store is reached with `datastore_context.get()`, which builds itself from
  the forwarded `METAFLOW_*` config — viable because gap 9 established that the
  pod can read Outerbounds' datastore.
- A failed load raises rather than warning: the body is about to read a path
  that would not be there.
- **`current.model.save()` is still refused**, with a message pointing at a
  plain artifact instead. Saving needs write access to the model store from
  the pod, which is a separate piece of work.

### 8. `current.huggingface_hub` / `@huggingface_hub` — ✅ read path; persist refused
- **Uses**: 17 sites.
- Same shape as `@model` — the decorator downloaded on the driver — but harder
  underneath: the registry hangs off `@checkpoint`'s task-scoped
  `CurrentCheckpointer`, which does not exist in the runner, so it cannot
  simply be rebuilt the way `LoadedModels` can.
- **Now**: `@huggingface_hub(load=[...])` is dropped from the driver and the
  repos are fetched in the pod, exposing `current.huggingface_hub.loaded` and
  the `load(...)` context manager. Entries may be a bare `repo_id` or a dict
  of `snapshot_download` arguments; `revision` and the pattern filters are
  carried through.
- **The source changes, and that is announced.** The driver serves from the
  datastore cache and falls back to the Hub; the pod goes to the Hub directly.
  For a repo without a pinned `revision` those are not guaranteed to be the
  same content, so the switch is logged rather than left to be discovered:

  ```
  [remote_step] @huggingface_hub: downloading from the Hugging Face Hub
                (the driver would have served these from the datastore cache)
  ```

  Pin `revision` if that matters. Reinstating the datastore cache means
  rebuilding the checkpointer in the pod — the deeper fix, not done.
- `current.huggingface_hub.snapshot_download()` is refused: it persists into
  the datastore, which the pod cannot write to. Same boundary as
  `current.model.save()`.

### 9. Metaflow client inside step body — ✅ (it already works)
- **Uses**: 30+ sites of `Flow(...).latest_successful_run`, `Run(pathspec=...)`,
  `Task(...)`, `namespace(...)`, `default_namespace()`.
- **The concern was wrong.** This entry assumed the pod would lack IAM
  permission on Outerbounds' Metaflow datastore. Probed directly from a runner
  pod (`ClientFlow` run 238592) and both halves answer:

  ```
  probe: metadata_service -> ok, latest_run=238592
  probe: datastore_read   -> ok
  ```

  The metadata service is reached over HTTP with the forwarded
  `METAFLOW_SERVICE_*` config, and the datastore read succeeds too — so no
  cross-account role is needed. No work required; keep the probe flow around
  to catch a regression if Outerbounds changes how the datastore is served.

### 10. `current.run.add_tags(...)` — ✅
- **Uses**: 7 sites.
- **Was**: `current.run` resolved to the no-op placeholder `__getattr__` hands
  out, so the call did nothing at all — silently.
- **Now**: the pod gets a recorder in place of `current.run` that captures
  add / remove / replace calls and writes them beside the outputs; the driver
  replays them after the step succeeds. Failing to apply a tag never fails the
  step — the body has already run and a tag is metadata.
- Reads (`run.data`, `run.tags`) are deliberately *not* faked. They would need
  a real client, and gap 9 shows one can be built in the pod if a use case
  turns up — better than returning a lie.
- Verified live: `ClientFlow` run 238594 —
  `applied run tags from the step: +['gap10-from-pod']`, and the tag is on the
  run in `end`.

---

## Major functional gaps — degrade UX / semantics

### 10b. Unpickling an artifact needs the same library, same version — ✅ documented
- Outputs travel as pickles, which store a *reference* to the type, not its
  code. So the step reading an artifact needs the defining library installed,
  at the same version that wrote it. Standard pickle behaviour, not a defect.
- **Rule: across step boundaries return plain types** (`str`, `int`, `dict`,
  `list`, DataFrame), or declare the library at the same version in every step
  that reads the artifact.
- `@uv_pypi_base` satisfies this by construction — every step derives from one
  `uv.lock`. The exposure is step-scoped `@pypi(packages=...)`, where one step
  has a library the others do not:

  ```
  ArtifactLoadError: unpickle failed for s3://.../report.pkl:
    No module named 'torch'
  ```

  Hit while writing the GPU probe: the report dict looked like plain data but
  held `torch.__version__`, which is a `TorchVersion`, not a `str`. `str(...)`
  fixed it.
- Version *mismatch* is the quieter case, since pickle records no version and
  checks nothing: usually fine, sometimes raises, occasionally rebuilds a
  subtly wrong object. numpy 2.0's `numpy.core` → `numpy._core` rename and
  pandas 2.x → 1.x are the known instances.

### 7. `current.model` / `@model(load=[...])` — ✅ load; save still refused
- **Uses**: 19 sites (embedding models, sklearn, spaCy, `distilbart_mnli_12_3`, etc.).
- **Bug**: `@model` downloads model artifacts on the driver argo pod, populates
  `current.model.loaded[...]`. Batch container has neither the files nor the
  populated dict.
- **Now**: the download happens in the pod, not on the driver, and only the
  *names* travel in the spec. That works because the model reference is an
  ordinary flow artifact — `@model` resolves it with `getattr(flow, name)` —
  which the spec already ships as an input, so the pod has everything it needs
  to fetch the model itself.
- `@model` is **dropped from the driver**, the same way `@kubernetes` is.
  Otherwise its `task_pre_step` downloads a multi-GB model onto a Small-tier
  pod with 10 GB of disk that never reads it. Now the file lands next to the
  GPU and never crosses the driver.
- The store is reached with `datastore_context.get()`, which builds itself from
  the forwarded `METAFLOW_*` config — viable because gap 9 established that the
  pod can read Outerbounds' datastore.
- A failed load raises rather than warning: the body is about to read a path
  that would not be there.
- **`current.model.save()` is still refused**, with a message pointing at a
  plain artifact instead. Saving needs write access to the model store from
  the pod, which is a separate piece of work.

### 8. `current.huggingface_hub` / `@huggingface_hub` — ✅ read path; persist refused
- **Uses**: 17 sites.
- Same shape as `@model` — the decorator downloaded on the driver — but harder
  underneath: the registry hangs off `@checkpoint`'s task-scoped
  `CurrentCheckpointer`, which does not exist in the runner, so it cannot
  simply be rebuilt the way `LoadedModels` can.
- **Now**: `@huggingface_hub(load=[...])` is dropped from the driver and the
  repos are fetched in the pod, exposing `current.huggingface_hub.loaded` and
  the `load(...)` context manager. Entries may be a bare `repo_id` or a dict
  of `snapshot_download` arguments; `revision` and the pattern filters are
  carried through.
- **The source changes, and that is announced.** The driver serves from the
  datastore cache and falls back to the Hub; the pod goes to the Hub directly.
  For a repo without a pinned `revision` those are not guaranteed to be the
  same content, so the switch is logged rather than left to be discovered:

  ```
  [remote_step] @huggingface_hub: downloading from the Hugging Face Hub
                (the driver would have served these from the datastore cache)
  ```

  Pin `revision` if that matters. Reinstating the datastore cache means
  rebuilding the checkpointer in the pod — the deeper fix, not done.
- `current.huggingface_hub.snapshot_download()` is refused: it persists into
  the datastore, which the pod cannot write to. Same boundary as
  `current.model.save()`.

### 9. Metaflow client inside step body — ✅ (it already works)
- **Uses**: 30+ sites of `Flow(...).latest_successful_run`, `Run(pathspec=...)`,
  `Task(...)`, `namespace(...)`, `default_namespace()`.
- **The concern was wrong.** This entry assumed the pod would lack IAM
  permission on Outerbounds' Metaflow datastore. Probed directly from a runner
  pod (`ClientFlow` run 238592) and both halves answer:

  ```
  probe: metadata_service -> ok, latest_run=238592
  probe: datastore_read   -> ok
  ```

  The metadata service is reached over HTTP with the forwarded
  `METAFLOW_SERVICE_*` config, and the datastore read succeeds too — so no
  cross-account role is needed. No work required; keep the probe flow around
  to catch a regression if Outerbounds changes how the datastore is served.

### 10. `current.run.add_tags(...)` — ✅
- **Uses**: 7 sites.
- **Was**: `current.run` resolved to the no-op placeholder `__getattr__` hands
  out, so the call did nothing at all — silently.
- **Now**: the pod gets a recorder in place of `current.run` that captures
  add / remove / replace calls and writes them beside the outputs; the driver
  replays them after the step succeeds. Failing to apply a tag never fails the
  step — the body has already run and a tag is metadata.
- Reads (`run.data`, `run.tags`) are deliberately *not* faked. They would need
  a real client, and gap 9 shows one can be built in the pod if a use case
  turns up — better than returning a lie.
- Verified live: `ClientFlow` run 238594 —
  `applied run tags from the step: +['gap10-from-pod']`, and the tag is on the
  run in `end`.

---

## Major functional gaps — degrade UX / semantics

### 10b. An artifact's *type* must exist wherever it is loaded — ⚠️ by design
- Outputs travel as pickles, so loading one needs the module that defines its
  type. A step-scoped `@pypi` package is installed only in that step's pod, so
  an artifact carrying one of its types cannot be read anywhere else:

  ```
  ArtifactLoadError: unpickle failed for s3://.../report.pkl:
    No module named 'torch'
  ```

  Hit while writing the GPU probe: the report dict looked like plain data but
  held `torch.__version__`, which is a `TorchVersion`, not a `str`. The fix in
  user code is `str(...)` — return plain types, or declare the package in every
  step that reads the artifact.
- **Importability is necessary but not sufficient.** Pickle records no version
  and checks nothing, so a step that *has* the module can still be wrong:

  | | outcome |
  |---|---|
  | module missing | `ModuleNotFoundError` — loud, names the module |
  | class moved or renamed, or its `__reduce__`/signature changed | raises |
  | attributes added or dropped with no `__setstate__` to reconcile | **loads a subtly wrong object, silently** |

  The third is the one to fear. Known instances: numpy 2.0 renamed
  `numpy.core` to `numpy._core` and broke pickles across that boundary; pandas
  2.x pickles do not load on 1.x; scikit-learn documents cross-version model
  unpickling as unsupported.
- **`@uv_pypi_base` makes this a non-issue for most flows.** Every step derives
  from the same `uv.lock`, so versions match by construction. The exposure is
  step-scoped `@pypi(packages=...)`: that step's set differs from the rest, so
  either the module is absent downstream, or — worse — the same package is
  pinned differently in two steps and you are in the silent row above.
- Practical rule: return plain data across step boundaries, or declare the
  package *at the same version* in every step that touches the artifact.
- Not fixable in the decorator: it is how pickle works. Worth knowing because
  the failure surfaces in the *downstream* step, far from the cause.
- **Could be made detectable.** The ref already carries `type_kind`
  (`module.QualName`) and the spec carries the producing step's packages, so
  recording the producer's version of the defining distribution and comparing
  it on load would turn the silent row into a warning. Not built — worth doing
  if anyone gets bitten.
- Same shape as the model-object cases in gaps 7 and 8.


### 11. `@environment(vars={...})` — ✅
- **Uses**: 1 site (rare but real).
- **Was**: Metaflow set those vars on the driver pod, which the runner does not
  inherit from — only the `METAFLOW_* / OBP_* / OUTERBOUNDS_* / GITHUB_TOKEN`
  allow-list reached it.
- **Now**: a sibling `@environment`'s `vars` are read at step_init and applied
  to the runner's environment last, so an explicit `@environment` wins over
  the forwarded Outerbounds context. Values are stringified; None is dropped
  rather than becoming the string "None".
- Verified live: `SibsFlow` run 238585 — the step asserts on its own env vars
  and passed.

### 12. `@catch(var="e")` preserving original exception — ✅
- **Uses**: 6 sites.
- **Was**: `@catch` sits on the driver task, so it only ever caught the
  poller's `RunnerError`; the user's exception was reduced to log text.
- **Now**: the runner pickles the exception (with its formatted traceback) to
  `<output_prefix>/exception.pkl` and the driver re-raises it before falling
  through to `RunnerError`. An exception that will not pickle — one holding a
  socket or a thread — degrades to type-and-message rather than failing the
  failure handler.
- Verified live: `SibsFlow` run 238585 — `caught` holds the flow's own
  `KnownFailure`, message intact.

### 13. `@timeout` sync driver ↔ runner — ✅
- **Uses**: 126 sites.
- **Was**: `@timeout` bounded only the driver. When it fired, the driver was
  killed while the runner pod carried on running — and billing — against
  `job_timeout_minutes`, which knew nothing about the user's intent.
- **Now**: a sibling `@timeout` becomes the Job's `activeDeadlineSeconds`,
  exactly. With no `@timeout` the decorator's own `job_timeout_minutes` still
  applies.
- **Never extended.** An earlier version added 5 minutes of slack, on the
  theory that the driver should outlive the pod and report the timeout. That
  was backwards: Metaflow kills the driver at the user's value, so a longer Job
  deadline left the pod running — and billing — with nobody watching, which is
  the very thing this entry exists to fix. The two now expire together, so
  which side reports it is a race (usually Metaflow's driver timeout). Both
  stop the work.
- Verified live through Argo — `argo-timeoutflow-822rv`, a step asking for
  `@timeout(minutes=1)` and then sleeping 6:

  ```
  rs-timeoutflow-slow-60167ed602-0  activeDeadlineSeconds= 60   # was 14400
  rs-timeoutflow-slow-60167ed602-0  Failed  0/1  2m23s          # killed, not sleeping
  ```

  Before the fix that field carried the 240-minute default, so the pod outlived
  its driver by hours.

### 14. `@gpu_profile()` — ✅
- **Uses**: 8 sites (advertising CR flows).
- **Was**: the decorator samples on the driver, which has no GPU, so a remote
  GPU step was profiled as an idle machine.
- **Now**: when a sibling `@gpu_profile` is present, the runner samples for the
  duration of the body using Outerbounds' own `GPUMonitor` (an `nvidia-smi -l`
  subprocess), and exposes the result as the `gpu_profile_data` artifact — the
  same name the decorator uses — plus a peak-utilisation line per device so the
  log alone answers "was the GPU actually used". Sampling also stops and
  reports when the body raises.
- **Detection is verified live.** `@gpu_profile` is a `StepMutator`: by the
  time `step_init` runs it has rewritten itself into a `card(type="blank",
  id="gpu_profile")` plus a `user_step_decorator`, so **nothing in the
  decorator list is named `gpu_profile`** and the obvious name match finds
  nothing — which is exactly how the first attempt failed silently. Detection
  keys off the injected card id instead, confirmed on `GpuDetectFlow` run
  238612 whose uploaded spec carries `gpu_profile: True`.
- A user `interval=` is **not** propagated: the mutator gives it to the
  wrapper and the card only gets `refresh_interval = max(5, interval)`, which
  is not invertible. Sampling falls back to 1 s — the decorator's own default
  and the finest setting, so nothing is missed.
- The wrapper cannot be dropped either, for the same reason it cannot be
  found, so it keeps running on the driver and writing `gpu_profile_data`.
  The runner therefore writes **`remote_gpu_profile`**, which the driver's
  empty reading cannot clobber, and appends its summary to the `gpu_profile`
  card through the card recorder so that card shows real numbers.
- Verified live on `GpuFlow` run 238624:

  ```
  gpu_profile: sampling 1 device(s) every 1s — driver 580.159.03, CUDA 13.0
  gpu_profile 00000000:31:00.0: peak 3% util, peak 274 MB memory, 7 samples
  gpucheck finished, 2 artifact(s) linked
  ```

- One trap worth knowing: `create_new_monitor()` only spawns `nvidia-smi -l`,
  which appends to a CSV. Nothing parses that file until `_update_readings()`
  runs, so `read()` returned `{}` and no artifact was produced — the first run
  showed sampling start and then linked only one artifact.
- The `gpu_profile` **card is cleared before the replay**. The wrapper fills it
  on the driver at task start — "Drivers: unknown / unknown", "No GPU devices
  found" — and cannot be dropped, so without clearing the card shows those
  blanks above the real numbers.
- **The sampler must finish before the card components are saved.** Its summary
  is appended inside `finish()`, and saving first left that summary unrecorded —
  the card then showed only the driver's blanks, which read as "the whole
  feature is broken" when the sampling was fine. Card confirmed clean on
  `GpuFlow` run 238632: zero `unknown` strings, real peak util and memory.
- **Read `remote_gpu_profile`, not `gpu_profile_data`.** The card's own
  "Detailed data saved in artifact gpu_profile_data" line is static text from
  the wrapper and points at the driver's empty artifact. `_gpu_profile_wrapper` renders everything
  through `current.card["gpu_profile"]`, and a card written in the pod does not
  reach the driver's card. So the readings exist but the chart does not. Gap 6
  is the keystone here, not extra GPU work.
- Degrades quietly by design: no GPU visible, or no profiler in the image, logs
  a line and carries on rather than failing the step.
- **`@gpu_profile` is dropped from the driver.** It samples wherever it runs,
  which for a remote step is a pod with no GPU, and then writes its own
  `gpu_profile_data` at `task_finished` — *after* the runner's outputs are
  applied. So the driver's empty reading silently replaced the real one. Seen
  live on `GpuFlow` run 238606, where the step body found the L4 and ran a
  matmul on it while the artifact said:

  ```
  {'error': 'nvidia-smi not found', 'devices': [], 'profile': {}}
  ```

  The give-away was the shape: that is the decorator's own artifact (`profile`
  key), not the runner's (`info` / `readings`).

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

### 16. `@conda` / `@conda_base` — ✅ (refused, by design)
- **Uses**: 2 sites (promo-lift flows).
- The runner builds its venv from `@pypi` / `@pypi_base` / `@uv_pypi_base`
  only, so a `@conda` step would run in an environment quietly missing its
  dependencies. It is refused at flow init with a message naming the
  alternative.
- **The name alone is not the signal.** `CondaEnvironment.decospecs()` returns
  `("conda",)`, so `--environment=pypi|conda|fast-bakery` attaches a bare
  `conda` decorator to *every* step to run the task lifecycle. Matching on the
  name refuses every flow that uses fast-bakery — which is all of them, as the
  first live run showed. The refusal keys on the decorator carrying non-empty
  `packages` or `libraries`, which the lifecycle one never does.

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

> **Every gap fix above was also verified through Argo, not only `run`.** That
> matters because `step_init` executes at *template-render* time on Argo as
> well as in the pod — so the decorator drops (`@kubernetes`, `@model`,
> `@huggingface_hub`, `@gpu_profile`) could in principle change the generated
> template — and because Argo builds foreach and join inputs by a different
> route (`--input-paths`, split indexes) than a local run.
>
> Neither turned out to be a problem:
>
> | flow | Argo run | covers |
> |---|---|---|
> | `GapsFlow` | `argo-gapsflow-nrckb` | foreach `self.input` (3 branches) + join + merge_artifacts |
> | `CardFlow` | `argo-cardflow-d8g46` | `current.card` replay |
> | `SibsFlow` | `argo-sibsflow-mqpjc` | `@environment`, `@catch` original exception |
> | `ProjFlow` | `argo-gapcheck.prod.projflow-mrdbj` | `@project` / `is_production`, deployed `--production` |
>
> All four finished successfully.


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

### R4. GPU workloads — ✅ (no separate image needed)
- **A `remote-step-runner-gpu` image turned out to be unnecessary.** The plan
  was to build one carrying CUDA / cuDNN. Tested on the existing
  `python:3.12-slim` runner instead (`GpuFlow`, run 238593, `g6` / L4):

  ```
  gpucheck: nvidia_smi    -> NVIDIA L4, 580.159.03, 23034 MiB
  gpucheck: torch_version -> 2.9.1+cu128
  gpucheck: cuda_available -> True
  gpucheck: matmul_on_gpu -> ok
  ```

  Two things make that work: Bottlerocket's NVIDIA variant injects the driver
  and `nvidia-smi` into any container that requests `nvidia.com/gpu`, and
  framework wheels (torch's `cu128` build here) bundle their own CUDA runtime.
  So a GPU step needs nothing but `@resources(gpu=N)` and its framework in
  `@pypi`.
- Karpenter provisioned the node in ~45 s from a cold start.
- `cpu_arch` defaults to `arm64`, and a GPU ask overrides it automatically —
  the GPU NodePool is amd64 only, so a step requesting a GPU is moved to
  x86_64 rather than failing on a default it never chose. Writing
  `cpu_arch="arm64"` *explicitly* alongside a GPU is still an error, since that
  asks for something that cannot exist.
- Constraints that do apply: an explicit `cpu_arch="arm64"` with `gpu>0` is
  refused (the
  gpu NodePool is amd64 only), and `content` is currently the only team with
  GPU quota, so a GPU step elsewhere stays Pending until one is granted.
- A dedicated image would only be worth building for a framework that expects
  system CUDA rather than bundling it — TensorFlow being the likely case.

---

## Coverage-status snapshot

Broken down by transition / decorator, counted across both production repos.

| Feature | Uses | Status |
|---|---:|---|
| Linear `self.next(a)` | ~925 | ✅ |
| Split `self.next(a, b)` | 5 | ✅ |
| Split-switch `self.next({..}, condition=...)` | 16 | ✅ (F23) |
| Foreach `self.next(a, foreach="x")` | 43 | ✅ incl. `self.input` (gap #3) |
| Foreach parallel `num_parallel=` | 0 | not needed |
| Join `def join(self, inputs)` | 58 | ✅ gap #1 |
| `self.merge_artifacts(inputs)` | 26 | ✅ gap #2 |
| `@step` | 1131 | ✅ |
| `@resources` | 112 | ✅ |
| `@kubernetes` | 372 | ✅ (dropped + our small kube injected) |
| `@retry` | 404 | ✅ retries the driver, which re-submits — intended |
| `@card` | 220 | ✅ gap #6 |
| `@secrets` | 97 | ✅ |
| `@timeout` | 126 | ✅ gap #13 |
| `@catch` | 6 | ✅ gap #12 |
| `@pypi` / `@pypi_base` | 92 | ✅ |
| `@environment` | 1 | ✅ gap #11 |
| `@model` | 19 | ✅ load, gap #7 |
| `@huggingface_hub` | 17 | ✅ read, gap #8 |
| `@gpu_profile` | 14 | ✅ gap #14 |
| `compute_pool` (kwarg on `@kubernetes`) | 10 | ✅ gap #15 |
| `@conda` / `@conda_base` | 2 | ✅ refused by design, gap #16 |
| `@batch` | 0 | 🚫 refused |
| `@parallel` | 0 | 🚫 refused |
| `current.is_production` | 209 | ✅ gap #4 |
| `current.card` | 66 | ✅ gap #6 |
| `current.run_id` | 40 | ✅ |
| `current.model` | 18 | ✅ load, gap #7 |
| `current.huggingface_hub` | 17 | ✅ read, gap #8 |
| `current.flow_name` | 14 | ✅ |
| `current.run.add_tags(...)` | 7 | ✅ gap #10 |
| `current.branch_name` / `project_name` | 4 | ✅ gap #5 |
| `current.step_name` | 1 | ✅ |
| `current.pathspec` | 1 | ❌ (derive from run_id/step_name/task_id) |
| `current.namespace` (custom) | 1 | ❌ ProjectFlow-provided |
| `Config` | 152 | ✅ |
| `Parameter` | 105 | ✅ |
| `IncludeFile` | 2 | ⚠️ untested |
| `self.merge_artifacts` | 26 | ✅ gap #2 |
| `self.input` (foreach) | 100+ | ✅ gap #3 |
| `Flow(...)`, `Run(...)`, `Task(...)` inside step body | 30+ | ✅ gap #9, verified |
| `parallel_map` inside step body | 17 | ✅ (works if metaflow installed) |
| `config_expr(...)` at decorator-arg time | 1 | ✅ (evaluated at flow init) |
