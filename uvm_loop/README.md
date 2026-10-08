# uvm_loop — Cocotb + pyUVM verification pipeline

Part of [ARGUS](../README.md) — Agentic RTL-to-GDS Using UVM Sign-off. This is
the UVM verification stage.

Give it RTL, a specification, and a Python reference model. It generates a
verification plan, builds a Cocotb + pyUVM testbench, runs it against the RTL
under Verilator, checks results against the reference model, and iterates —
repairing the testbench and the RTL until the design is verified.

```bash
python3 -m pipeline.run14 --design-config benchmarks/fifo/design.yaml
```

Run it as a module from `uvm_loop/` — imports are `pipeline.*`, `src.*`,
`uvm_generator.*`, and design YAML paths are repo-relative.

## Inputs

| Input | Notes |
|---|---|
| RTL | The SystemVerilog implementation under verification |
| Specification | The design's **intended** behaviour — not what the current RTL does. The loop may modify RTL to match it. |
| Reference model | A Python model of expected behaviour, used for scoreboarding |

Verification infrastructure is generated from the spec and reference model
rather than from the RTL, so a mismatch is attributed to the RTL instead of
being baked into the testbench.

## Layout

```text
uvm_loop/
├── benchmarks/<design>/     # RTL + spec + reference model + design.yaml
├── pipeline/
│   ├── run14.py             # the orchestrator and entrypoint
│   ├── functions.py         # Ray-dispatched remote operations
│   ├── tb_feedback.py       # testbench diagnosis / repair
│   └── verification_improvement.py
├── src/                     # RTL parsing, plan generation, VerificationPlan schema
├── uvm_generator/           # renders templates/*.j2, validates output
├── workers/{rtl,sim}/       # worker images
├── config/
│   ├── llm_models.txt       # ordered LLM fallback list
│   └── pyuvm_api_reference.md   # generated; inlined into every prompt
├── scripts/                 # add_llm_provider.sh and helpers
├── tests/                   # pytest suite
├── generated/               # all run output (gitignored)
├── cluster.yaml
├── setup.sh
├── run_forever.sh           # supervisor
└── Makefile
```

You normally touch only `benchmarks/`, `cluster.yaml` and `pipeline/run14.py`. `uvm_generator/verilator_compat.py` rejects
SystemVerilog constructs Verilator can't handle (e.g. parameterised virtual
interfaces); `rules.yaml` and `capabilities.yaml` constrain generation.

## Setup

Prerequisites: Linux, Docker, the `chia` CLI, and SSH access between the
cluster host and workers. All simulation tooling runs inside the workers —
nothing to install on the host beyond `chia_env`.

```bash
conda activate chia_env

docker build -t chia-rtl-worker:local -f workers/rtl/Dockerfile .
make build-sim-image        # chia-sim-worker:chia-local
```

Then in `cluster.yaml`, set `auth.ssh_private_key` to your key, and check that
the opencode worker's `auth.json` mount points at your real credentials file.

```bash
./setup.sh                  # verifies prerequisites, picks a provider, runs chia up
chia status && ray status
```

`setup.sh` prompts for a provider. Use **OpenCode** with `opencode/big-pickle`;
long runs make hundreds of model calls and a generous limit keeps a run from
stalling partway. For Gemini, export `GOOGLE_GENERATIVE_AI_API_KEY` first.

## Verifying your own design

The whole path — spec to signed-off GDS — is one script:

```bash
../scripts/run_full_flow.sh my_fifo path/to/my_fifo.sv path/to/spec.md path/to/ref_model.py
```

To do just the UVM stage, or for more control:

**1.** Create `benchmarks/my_fifo/` with `my_fifo.sv`, `spec.md` and
`ref_model.py`. The spec should cover inputs/outputs, widths, reset and clock
behaviour, handshakes, expected outputs, corner cases and timing requirements.
Don't copy an existing generated testbench in — the environment is meant to be
generated from the spec.

**2.** Add `benchmarks/my_fifo/design.yaml` beside them (copy
`benchmarks/TEMPLATE.yaml`). Paths inside it resolve relative to `uvm_loop/`,
not to the config's own location:

```yaml
name: my_fifo
rtl: benchmarks/my_fifo/my_fifo.sv
spec: benchmarks/my_fifo/spec.md
ref_model: benchmarks/my_fifo/ref_model.py
output_parent: generated/designs
```

**3.** Run it with `--design-config benchmarks/my_fifo/design.yaml`. Output
lands in `generated/designs/my_fifo/` — `rtl/`, `plans/`, `tb/`, `results/`,
`tb_iterations/`, `checkpoints/` and `rtl_verification/`.

Re-running **resumes** from valid artifacts already in that directory,
including `improvement_state.json`, so it is safe to re-run after an
interruption. Delete the directory for a genuinely clean run (`make clean`
wipes all designs).

## Long runs

Use the supervisor rather than calling `run14` directly:

```bash
./run_forever.sh benchmarks/fifo/design.yaml 25
```

The second argument caps verification-improvement iterations. The supervisor
restarts `run14` on unexpected failure, preserves generated state between
attempts, and stops once `run14` exits successfully.

Logs: `generated/designs/<design>/results/verification_improvement/logs/`.

## Multi-model LLM fallback

`opencode/big-pickle` is a **shared pool across all free-tier opencode-zen
users**, not a per-account quota. A one-off prompt in your terminal can
succeed while a pipeline run gets `429` at the same moment, because the
pipeline issues far more concurrent requests. So `run14` picks its model from
an ordered fallback list instead of one hard-coded provider.

| File | Role |
|---|---|
| `config/llm_models.txt` | Ordered `provider/model` candidates. `#`-comments record why a model is excluded, and flag the paid ones. |
| `generated/llm_model_state.json` | `"current"` plus a `"cooldown_until"` map. Safe to hand-edit — clear an entry to force a re-pick. |
| `generated/llm_model_usage.jsonl` | Append-only log of which model produced which run. |

At startup `_load_llm_model()` picks `state["current"]` if it isn't cooled
down, else the first candidate that isn't. On a rate limit or provider error
the current model is cooled down and the process exits non-zero.

How long it is benched depends on what failed:

| Failure | Cooldown | Examples |
|---|---|---|
| Transient — provider briefly unwell | `LLM_TRANSIENT_COOLDOWN_S`, 180 s | `Internal error encountered`, `experiencing high demand`, `Service temporarily overloaded` |
| Persistent — provider unusable | `LLM_RATE_LIMIT_COOLDOWN_S`, 1 h | 429 rate limits, auth failures, a model returning 404 |

The split exists because a flat hour is wrong for a blip: on 2026-09-23 two
Google 500s benched both paid Gemini models — deliberately first in the list —
for an hour each, while the run fell back to free models nobody chose. Both
recovered in seconds.

Transient markers are also part of `_MODEL_UNAVAILABLE_MARKERS`. They have to
be: a text matching no marker is treated as ordinary bad output, so the model
is never cooled down *or* switched away from. The two lists answer different
questions — *is this the provider's fault* and *for how long*.

**Fallback only engages under the supervisor.** A bare `run14` process reads
the model once at startup and keeps it for its lifetime, so a rate limit just
kills the run. Only a restart re-reads the state file. `orfs_loop.py` takes a
single-shot `--model` flag and does not share this mechanism.

Every LLM call is bounded by `LLM_CALL_TIMEOUT_S` (default 1800 s), and
aborted sooner when opencode's own log already shows the provider failed —
`LLM_FAILFAST_POLL_S` / `LLM_FAILFAST_GRACE_S`, `0` to disable. A provider
that stalls silently — returning nothing, raising nothing — would otherwise
block forever and never reach the fallback path, so the timeout is converted
into a recognised failure: the model is cooled down and the process exits
non-zero, which is what lets the supervisor restart on the next model. See
[Troubleshooting #2](../docs/TROUBLESHOOTING.md#2-llm-provider-stalls-mid-call).

### Paid models

The list is free-tier by default. `google/gemini-*` are the exception — they
bill on every call, using the `google` key in `auth.json` (Google AI Studio;
opencode resolves it by provider name, so no `opencode.jsonc` entry is needed).
Measured 2026-09-23: about **$0.27 per 1M input tokens**, so a single long
agentic call can cost over a dollar and an unattended overnight run is real
money. Their position in the list is the whole control — at the top they bill
on essentially every call, at the bottom only when every free model is cooled
down. Moving them is a one-line edit.

Note that `_load_llm_model()` honours `state["current"]` ahead of list order,
so a reordering does not take effect until that key is cleared:

```bash
python3 -c "import json;p='generated/llm_model_state.json';d=json.load(open(p));d['current']=None;json.dump(d,open(p,'w'),indent=2)"
```

### Adding a model or key

```bash
scripts/add_llm_provider.sh <base_url> <api_key> <model_name> [slug]
```

This picks the next free `nvidiaN` slot, registers it in `opencode.jsonc`,
syncs it into every running opencode container, smoke-tests it live, and adds
it to `config/llm_models.txt` only if the test passes. Doing this by hand
spans three files (host config plus two containers) and is easy to get subtly
wrong — a missed container, a wrong slot, or an untested model landing
straight in the live fallback list.

If you must do it manually: add the provider to `~/.config/opencode/opencode.jsonc`
with its key inlined in `options.apiKey` (custom providers do **not** inherit
`auth.json` keys), `docker cp` the file into each opencode container (that path
is not bind-mounted, unlike `auth.json`), smoke-test with a 60–90 s timeout
(some models are slow only on a cold first call — retry once before excluding),
then add the verified line to `config/llm_models.txt` with a dated comment.

## Prompt cost: the pyuvm API reference

pyuvm 5.0.0's API differs from older pyuvm and from SystemVerilog UVM in ways
the model does not reliably know, so it can fall into reverse-engineering the
library at runtime. On one measured call, **71% of the agent's shell tool calls
were `grep` / `sed` / `inspect` against pyuvm in site-packages**, and because
the agentic loop re-sends its accumulated context on each step, that call
reached 6.08M tokens over 83 steps.

This is rare, not routine: across the 39 agent streams retained on the workers,
that one call had 51 site-packages hits and the other 38 had 0-6. Treat the
reference as cheap insurance against a recurrence rather than a routine saving
— see [Troubleshooting #12](../docs/TROUBLESHOOTING.md#12-one-llm-call-costs-millions-of-tokens).

`config/pyuvm_api_reference.md` holds those signatures instead (~12KB, ~3.1k
tokens), and `_hard_constraints()` in `run14.py` inlines it into all six
generation prompts, telling the agent not to read library source to re-confirm
them.

**Regenerate after any pyuvm upgrade** — it must be produced in a worker,
which is where pyuvm is installed:

```bash
docker exec chia-sim-$USER-0 python3 /workspace/scripts/gen_pyuvm_api_reference.py \
  > config/pyuvm_api_reference.md
```

The generator refuses to emit if the installed library contradicts its
hand-written preamble: it re-checks every claim against the live pyuvm and
*exercises* the risky ones (it really calls `raise_objection()`, really
round-trips a value through `ConfigDB`) rather than trusting signatures. A
stale reference would be the same trap as a dead orchestrator snapshot
(Troubleshooting #7), so failing loudly beats shipping a lie. If the file is
missing, `run14` prints a note and carries on with the old behaviour.

See [Troubleshooting #12](../docs/TROUBLESHOOTING.md#12-one-llm-call-costs-millions-of-tokens).

## Tests

```bash
python3 -m pytest tests/
python3 -m pytest tests/test_verilator_compat.py::test_comment_examples_are_ignored
```

`pytest` is not in the system Python — install it into `chia_env`. The
`python3 -m` form is what puts `uvm_loop/` on `sys.path`.

## Commands

| Task | Command |
|---|---|
| Run a benchmark | `python3 -m pipeline.run14 --design-config benchmarks/<design>/design.yaml` |
| Run via Make | `make pipeline BENCHMARK=<design>` |
| Run the supervisor | `./run_forever.sh benchmarks/<design>/design.yaml 25` |
| Build the sim image | `make build-sim-image` |
| Build the RTL worker | `docker build -t chia-rtl-worker:local -f workers/rtl/Dockerfile .` |
| Cluster health | `chia status && ray status` |
| Clean all designs | `make clean` |

## Setup problems

Runtime and pipeline failure modes are in
[docs/TROUBLESHOOTING.md](../docs/TROUBLESHOOTING.md). Setup-specific issues:

- **`chia: command not found`** — `conda activate chia_env`.
- **Docker not accessible** — check `docker info`; start the daemon, or add
  your user to the `docker` group.
- **Worker image not found** — rebuild it (see Commands above). Note the RTL
  worker's file is `workers/rtl/Dockerfile`.
- **Workers can't connect** — verify SSH access and the `ssh_private_key` path
  in `cluster.yaml`, and that both images exist. If the host IP changed,
  reconcile the cluster so `THIS_MACHINE` updates.
- **OpenCode auth failures** — check the host-side path of the `auth.json`
  volume mount in `cluster.yaml`.
