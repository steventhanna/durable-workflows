---
name: verify-invariants
description: Use for any change to durable-workflows engine behavior — fixing a gap, changing claim/lease/fence/retry/schedule/cancel logic, touching the Quint model or the trace recorder — and before a push or release. Runs the loop that keeps the code, docs/INVARIANTS.md, the Quint model and the recorded traces in agreement.
---

# Verify invariants

The loop that keeps this engine honest. `docs/INVARIANTS.md` is the fixed
point; the compiler, the tests, the Quint model and trace checking are four
views of it. A change is done when all four agree with the invariants, not
when the checks are green by any means.

## The rules that never bend

- **Invariants change only by decision.** Never edit an invariant in
  `docs/INVARIANTS.md` or in the model's `safety` set, and never add an
  exclusion or a `gaps.yaml` entry, to make a check pass. If the check shows the
  invariant is wrong, stop and ask the owner; record the decision in
  INVARIANTS.md and the commit.
- **Fix the code, not the test.** A failing test that looks wrong is reported,
  not edited (CLAUDE.md, "Test integrity").
- **Compiler first.** Before a runtime check or a test, try to make the
  violation unrepresentable or uncompilable (CLAUDE.md, "Rely on the compiler
  first"; the `rust-type-audit` skill helps find candidates).
- **Checks run one at a time.** `scripts/verify-invariants.sh` orders them;
  never run a trace pipeline alongside another test run or `check.sh`.

## The loop

### 1. Name the invariant
Find the invariant(s) in `docs/INVARIANTS.md` the change relies on or
changes (S*, L*, F*, the gap entry in §6). If there is none, the change is not
specified yet: write the invariant first (and ask the owner if it changes
behavior).

### 2. Reproduce before fixing (bugs and gaps)
- A Rust test that fails for the right reason on both backends
  (`durable-workflows/tests/gaps.rs`, `#[ignore = "confirms <gap>: ..."]` until
  fixed).
- The model shows it: a directed test in `spec/durable_tests.qnt`, or an
  invariant `check.sh` finds violated, and the gap's trace listed in
  `spec/traces/gaps.yaml` as an expected violation.
A fix without a reproduction proves nothing about the bug.

### 3. Change the code, the model and the docs together
- Code: the smallest change that restores the invariant, typed where possible.
- Model (`spec/durable.qnt`): state the new behavior; move the gap invariant
  into `safety`; invert the gap's directed test to assert the fixed behavior.
- Recorder/generator: if a recorded transaction changes shape, update
  `src/trace/` and `tools/durable-trace/src/gen.rs`, bump `TRACE_IFACE_VERSION`
  (model) and `IFACE_VERSION` (gen) together, and list it in `spec/README.md`.
- Un-ignore the gap test; remove its `gaps.yaml` entry.
- Docs: INVARIANTS.md (the invariant text, §6 status), README Known issues,
  CHANGELOG; CLAUDE.md examples when a new type-level pattern lands.

### 4. Run the checks
```sh
scripts/verify-invariants.sh            # every change
scripts/verify-invariants.sh --full     # before a push or release
```
Stages: fmt, clippy (4 feature sets), rustdoc, the test suites on MySQL and
Postgres (incl. trybuild compile-fail cases), the Quint model (typecheck,
directed tests, random simulation; `--full` adds 20000-sample simulation and
Apalache), and trace checking on each backend. Logs: `target/verify-invariants/`.

### 5. Classify every failure before changing anything
Read the log (for traces: `spec/trace-check.sh` names the first failing step;
`durable-trace report --trace <json> --qnt <module> --step <k>` diffs model
and recorded values). Then decide which of three is wrong:

| Class | Sign | Fix |
|---|---|---|
| (a) code | the recorded step breaks an invariant, or does something the invariants forbid | fix the code (step 3) |
| (b) recorder / generator | the model would accept the real behavior but the trace maps it wrongly (a missing field, a wrong id, an unrecorded write) | fix `src/trace/` or `gen.rs`; add a unit test there |
| (c) model | the code does what INVARIANTS.md says and the model disagrees | fix the model and say why in the commit; if INVARIANTS.md itself is unclear, ask |

Timing: a failure in a known timing-sensitive test (listed in the current
work plan or CLAUDE.md) is rerun alone once; if it passes alone, report it, do
not count it as fixed or broken.

Repeat 3–5 until every stage passes.

### 6. Baseline
When a change moves traces on purpose, run each backend's pipeline with
`--update-baseline` and read the `spec/traces/expected.json` diff: `pass` up and
`violation_confirmed` down by the gaps fixed, exclusions unchanged unless the
change explains them. CI fails when `pass` drops or an exclusion count rises. A
trace that flips between `pass` and an exclusion by timing keeps its exclusion
key at 1 and the lower `pass` count; name it in the commit.

### 7. Report
For each stage: the command and the result (counts per backend for tests and
traces). For each failure: its class and the fix. List every test edit and its
approval. Say what the checks do not cover (the simulation samples, Apalache is
bounded, traces cover only the executions the tests produce).
