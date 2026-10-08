# orfs_loop — ARGUS physical-design stage

Part of [ARGUS](../README.md) — Agentic RTL-to-GDS Using UVM Sign-off.

An LLM-driven RTL-to-GDS closure loop for [OpenROAD-flow-scripts](https://github.com/The-OpenROAD-Project/OpenROAD-flow-scripts)
(ORFS), built on the [chia](https://github.com/ucb-bar/chia) agent framework. An
LLM iteratively tunes a whitelisted set of `config.mk` knobs and re-runs the
flow until timing/DRC closure, subject to hard limits you set that it can
never override.

**First time on a machine that's never run this before?** You'll need Docker,
local SSH enabled (`chia` SSHes into itself even for a single-machine
cluster), a `chia_env` conda environment with the `chia` package installed,
the local Docker image built (`docker build -f Dockerfile.orfs-run -t
chia-orfs-run:local .`), and LLM provider auth configured. Everything below
assumes all of that already exists.

`orfs-native-build/` (the vendored ORFS toolchain) is a git submodule, so
clone with `--recurse-submodules` or initialize it after the fact:

```bash
git submodule update --init orfs-native-build
```

Note the sky130hd LVS fixes described under "Signoff" below are a host-local
patch to that checkout, not something the submodule pin carries -- they need
reapplying after a fresh clone or reset of `orfs-native-build/`.

## How it fits together

```
orfs_loop.py  (driver, runs on the host, owns the whole loop)
  -> run_flow_remote / apply_tunables_remote     direct blocking Ray calls,
       (orfs_tool.py, on an "orfs_run" Ray         no MCP, no tool-calling
        worker)
       -> make -C flow clean_all synth/floorplan/place/cts/route/finish/metadata
  -> OpenCodeLLM.prompt(..., tools=[])   one LLM turn per run: the model reads
       the flow result as a prompt and replies with a JSON tunable diff (or
       CLOSURE: PASS / CLOSURE: GIVE_UP) as plain text -- no tools, it never
       runs anything itself
```

This used to be the more obvious "give the LLM MCP tools and let it call
`get_tunables`/`set_tunables`/`run_full_flow_and_summarize` itself" shape. It
broke: opencode's MCP client times out a call at ~60s, but a real flow run
takes 90s-15min, so the client would retry mid-run and a second concurrent
flow execution would `make clean_all` out from under the first one,
corrupting both. The fix was removing the LLM's ability to call anything --
the driver now owns every flow/tunable call directly, and the model's only
interface is replying with text.

*Uncontrolled* overlap on the same path is therefore still structurally
impossible: nothing but the driver can start a flow run. The driver itself
does run flows concurrently when you ask it to (`--batch-size N`, below), but
each concurrent call gets its own ORFS `FLOW_VARIANT` and its own working
config, and ORFS scopes every output directory by `FLOW_VARIANT` -- so no two
in-flight runs ever share a path. That is a different thing from the
client-timeout retry storm the original design hit.

- **`orfs_loop.py`** -- the driver: connects to the Ray cluster, runs a
  two-level loop (**iterations** are the major unit -- iteration 1 ends the
  moment a run closes; each iteration after that must close again *and* beat
  the previous one's `--objective` value -- containing **runs**, the minor
  unit: one flow execution + one LLM turn, uncapped per iteration), and
  decides when to stop.
- **`orfs_tool.py`** -- home of the actual worker-side functions the driver
  calls: `read_tunables_remote`/`apply_tunables_remote` (read/validate/write
  the managed tunable block), `run_flow_remote` (the `make clean_all` +
  stage sequence), `run_signoff_remote` (standalone `make drc`/`make lvs`
  after a close). All `@ChiaFunction(resources={"orfs_run": 1})`. The file
  also still contains `ORFSTool`, an MCP-tool-server class from the old
  design -- unused by the current loop, kept around but dead code.
- **`orfs_config_bridge.py`** -- validates tunables against the schema and
  renders them into a managed block appended to a *copy* of your baseline
  `config.mk` (`config.chia.mk`). The baseline file is never edited in place.
- **`orfs_tunables_schema.json`** (and `orfs_tunables_schema_minimal.json`,
  a reduced 12-tunable variant, `--minimal-schema`) -- the closed whitelist:
  every key the LLM is allowed to touch, with type/range/enum bounds.
  Anything not listed here is invisible and unreachable to the LLM, full stop.
- **`orfs_ledger.py`** -- accumulates what every tunable change *measurably
  did*, across every run and every parallel slot, and renders it into each
  prompt as `measured_effects_so_far`. It is evidence, never a decision-maker.
  Two honesty rules it keeps: multi-knob moves are quarantined as "combined"
  rather than averaged into a per-knob effect, and `CLOCK_PERIOD` moves are
  flagged, since relaxing the clock improves slack by definition and would
  otherwise read as "raising CLOCK_PERIOD reliably fixes timing" -- true,
  useless, and exactly the trade `--objective` exists to prevent. Written to
  `ledger.json`; carry it into a later run with `--seed-ledger`.
- **`prompts/orfs_propose.md`** -- the LLM's entire interface: reads the
  current goal/schema/config/flow-result/history and replies with a fenced
  JSON tunable diff or a `CLOSURE:` sentinel. Its strategy section moves
  "free" knobs (repair flags/margins) aggressively but caps "target" knobs
  (`CORE_UTILIZATION`, `PLACE_DENSITY`, `DIE_AREA_SCALE`, `CLOCK_PERIOD`) to
  ~10-15% relative steps per turn, always -- otherwise a model chasing
  closure will happily double the die area or give back most of the clock
  speed to get there.
- **`prompts/orfs_referee.md`** -- one extra LLM turn per parallel round that
  reads every slot's result side by side and writes the cross-slot note the
  next round's calls all see. The only thing that reasons *across* slots, and
  the only thing positioned to call a plateau. Disable with `--skip-referee`.
- **`prompts/orfs_diagnose.md`** -- the stall-diagnosis turn, used only when
  the last 3 runs all came back REGRESSED/UNCHANGED.
- **`orfs_onboard.py`** -- one-time bridge to generate a `config.mk` for a
  fresh Tiny-Tapeout-style RTL repo that doesn't have one yet.
- **`orfs_gui.py`** / **`gui/index.html`** -- the dashboard. Runs
  `orfs_loop.py` as a subprocess per run and polls its `tool_trace.log`/
  `summary.json` for live progress.
- **`cluster.yaml`** -- the `chia` cluster topology: one Docker-backed Ray
  worker per resource type (`orfs_run`, `opencode_creds`, plus the chipyard/
  verilator example workers). `chia up`/`chia down` (from the `chia` package)
  read this, not raw `ray up`. Bind-mount paths are parameterized via
  `${CHIA_ORFS_REPO}`/`${HOME}`, not hardcoded to one person's machine.
  `hello_orfs.num_workers` (ships as `6`) is the `orfs_run` capacity, and
  therefore how wide `--batch-size` can actually run in parallel.
- **`Dockerfile.orfs-run`** -- builds the `chia-orfs-run:local` image the
  `orfs_run` workers use. Thin by design: `FROM openroad/orfs:latest` plus a
  conda/ray/`chia` layer and `netgen-lvs` -- it `COPY`s nothing from this
  repo, so the image never contains local edits. `orfs-native-build/` and the
  repo are bind-mounted at *runtime* instead.
- **`lvs/sky130hd_netgen_setup.tcl`** -- a netgen device-combination setup
  ported from open_pdks' `sky130_setup.tcl` (what OpenLane and Tiny Tapeout
  use for sky130 signoff), run as a **second opinion alongside** KLayout's
  LVS, never replacing it. See "Signoff" below.
## One-time setup

```bash
conda activate chia_env
cd <this directory>               # the orfs_loop/ directory, the one holding cluster.yaml
export CHIA_ORFS_REPO=$(pwd)      # cluster.yaml's bind-mount paths key off this
chia up cluster.yaml -y
```

`CHIA_ORFS_REPO` must point at **this** directory -- the one containing both
`cluster.yaml` and `orfs-native-build/` -- because `cluster.yaml` mounts
`${CHIA_ORFS_REPO}/orfs-native-build` at `/root/OpenROAD-flow-scripts` and
`${CHIA_ORFS_REPO}` itself at `/root/chia-orfs` inside every worker.

`cluster.yaml`'s `head_ip` and every node type's `compatible_ips` are pinned
to `127.0.0.1`, not a real network IP -- this is a single-machine cluster and
every worker runs `--net=host`, so loopback inside a container already is
this host. That makes chia's *own* orchestration (SSH targeting, docker
exec/run, the `RAY_HEAD_IP` workers join through) independent of the host's
real IP: no `THIS_MACHINE`-style env var to recompute, and `chia up
cluster.yaml -y` reconciles cleanly no matter what the host's current address
is. It does **not** make an already-running cluster survive that address
changing mid-session -- Ray's own node identity is a layer underneath
chia's, and Ray refuses to let a node advertise `127.0.0.1` as itself (it
substitutes the real outbound IP even if you pass `--node-ip-address
127.0.0.1` explicitly). See the GCS-timeout entry in Troubleshooting for what
that means in practice.

This brings up (or reconciles) the Ray head + one Docker worker per node type
in `cluster.yaml`. The ORFS workers are named `chia-orfs-<your-username>-0`,
`-1`, ... -- substitute your own username wherever the examples below show
`chia-orfs-<your-username>-0`.

Check it's up: `chia status` or `ray status` should show `orfs_run: 6.0` total
capacity with the shipped `num_workers: 6`.

**Editing `orfs_tool.py` / `orfs_config_bridge.py`?** No extra step needed --
the whole repo is bind-mounted into the container with `PYTHONPATH` pointing
at it, so changes are live immediately. (This used to require a manual
`docker cp`; that's gone now -- see cluster.yaml's `hello_orfs.docker.run_options`
if you're curious how.)

## The dashboard (easiest way to drive this)

```bash
python3 orfs_gui.py   # then open http://127.0.0.1:8080
```

Runs the FastAPI dashboard app directly. If your shell session predates
being added to the `docker` group, prefix this with `sg docker -c "..."`
(see the troubleshooting entry below) -- otherwise just make sure
`chia up cluster.yaml -y` has already been run in this same environment
first.

Everything the CLI does, without terminal juggling: pick a design and model
from dropdowns, set iterations, lock tunables, hit **Start**, and watch it
go. The page polls every 2.5s and shows:

- which flow stage is executing right now (`synth → floorplan → place → …`)
- a table of every flow run: what changed, the verdict
  (`IMPROVED`/`REGRESSED`/`CRASHED`), setup WNS/TNS, hold, DRC, utilization, area
- a WNS-vs-run chart with the zero line marked, so you can see closure approach
- crash banners showing the **real** OpenROAD error (e.g. `[ERROR FLW-0024]`),
  and the area-headroom warning when the die is too full to pad
- the agent's reasoning as one continuous transcript across the whole run,
  artifacts produced, and the tool trace log (`tool_trace.log`)

The run picker in the top right also opens any past run in `orfs_runs/`, so
you can review finished work with the same view.

Runs launch as ordinary `orfs_loop.py` subprocesses in their own session --
identical `run_dir` layout to a CLI run, and they keep going if you close
the browser. **Stop** terminates the active one.

One loop at a time: a second dashboard-launched run would compete for the
same `orfs_run` workers as the first. (Within a single run, `--batch-size`
does use several workers at once -- see below.)

## Running the closure loop (CLI)

Point it at a design that already has a `config.mk` under
`orfs-native-build/flow/designs/<platform>/<design>/` -- either one of ORFS's
built-in examples (`gcd`, `riscv32i`, `aes`, `jpeg`, `ibex`, `chameleon`,
`microwatt`, ...) or one you onboarded (below).

`--design-name` is a **label**, not a path -- the design itself comes from
`--design-config-mk`/`--reports-root`. But it keys the ledger's identity
(`--seed-ledger` refuses to carry a ledger across designs by comparing it) and
fills `$DESIGN_NAME` in every prompt, so a stale value mislabels both. It
defaults to `gcd`; pass it explicitly for anything else.

```bash
conda activate chia_env
cd <this directory>

DESIGN=/root/OpenROAD-flow-scripts/flow/designs/sky130hd/riscv32i

python3 orfs_loop.py \
  --design-name riscv32i \
  --design-config-mk $DESIGN/config.mk \
  --reports-root /root/OpenROAD-flow-scripts/flow/reports/sky130hd/riscv32i/base \
  --max-iterations 6 \
  --objective area \
  --model opencode/big-pickle
```

**Both paths must be container-native absolute paths** (`/root/OpenROAD-flow-scripts/...`),
not host paths -- the config-prep step and every remote call run inside the
`orfs_run` worker, which doesn't see your host filesystem except through the
two bind mounts above.

`--reports-root` is ORFS's own `reports/<platform>/<design>/<variant>`
directory (usually `.../base`), **not** a `results/` folder under the design's
own directory -- those are two different, sibling directories ORFS writes to.
Get this wrong and the flow runs fine but `run_flow_remote` reports
`"metadata.json missing"` at the end.

`--max-iterations` counts **iterations**, not runs: iteration 1 ends the
moment a run closes; each iteration after that must close again *and* beat
the previous one's `--objective` value. Runs within an iteration are
uncapped -- the model itself ends a non-closing iteration by replying
`CLOSURE: GIVE_UP`, or an un-improving `CLOSURE: PASS`.

### CLI reference

| Flag | Default | Notes |
|---|---|---|
| `--design-name` | `gcd` | A label, not a path -- it keys the ledger and fills `$DESIGN_NAME` in prompts. Pass it explicitly for anything but `gcd`. |
| `--flow-dir` | `/root/OpenROAD-flow-scripts/flow` | Rarely needs changing. |
| `--design-config-mk` | *required* | Container-native path to baseline `config.mk`. |
| `--reports-root` | *required* | Container-native path to `flow/reports/<platform>/<design>/base`. |
| `--run-dir` | `orfs_runs/<unix-ts>` | Where this run's artifacts land (see below). |
| `--max-iterations` | `3` | **Iterations**, not runs -- see above. |
| `--objective` | `area` | `area` or `clock`. What each iteration past the first must improve on. Ignored by iteration 1. |
| `--model` | `google/gemini-3.6-flash` | Any `opencode` model string. See "Choosing a model" below. |
| `--closure-criteria` | *(none)* | Overrides the closure target text given to the LLM. |
| `--minimal-schema` | off | Use the reduced 12-tunable schema instead of the full 24-tunable one. |
| `--batch-size` | `1` | Flow runs to execute concurrently per round. `1` = serial. See below. |
| `--skip-referee` | off | Skip the per-round cross-slot referee turn (only relevant with `--batch-size` > 1). |
| `--seed-ledger PATH` | *(none)* | Warm-start the measured-effects ledger from an earlier run's `ledger.json`. Refused across designs. |
| `--stage-timeout-seconds` | `3600` | Per-stage timeout. Raise it for anything bigger than a small example design. |
| `--prompt-path` | `prompts/orfs_propose.md` | The proposal prompt template. |
| `--diagnose-prompt-path` | `prompts/orfs_diagnose.md` | Stall-diagnosis prompt, used after 3 runs with no progress. |
| `--referee-prompt-path` | `prompts/orfs_referee.md` | Cross-slot referee prompt. |
| `--resume-config` | off | Continue from the previous run's tunables instead of resetting to baseline. |
| `--skip-signoff` | off | Skip standalone `make drc`/`make lvs` after a successful close. |
| `--lock KEY=VALUE` | *(none)* | Repeatable. Hard-locks a tunable -- see next section. |
| `--ld-library-path` | `/opt/miniconda/lib` | Needed by some ORFS-bundled binaries; leave as-is. |
| `--ray-address` | `auto` | Ray cluster address. |

**`--stage-timeout-seconds` matters more than it looks.** The 3600s default
was sized around `gcd`-scale designs. A bigger design's route stage can still
be genuinely converging when it fires -- observed on `aes`/sky130hd, down to
41 violations and actively shrinking when the default killed it at exactly the
3600s mark, wasting the whole hour. Raising it doesn't paper over anything (a
timeout is recorded as an ordinary crashed run either way), it just stops
every attempt on a slow design from being a guaranteed hour-long crash.

## Parallel batches (`--batch-size`)

```bash
python3 orfs_loop.py ... --batch-size 6
```

Instead of "one flow run, one decision, repeat", a **round** is "decide up to
N candidates, run all N concurrently, keep the best". Each slot gets its own
ORFS `FLOW_VARIANT` (`slot0`, `slot1`, ...) and its own
`config.chia.slotN.mk`/`constraint.chia.slotN.sdc`, which is what makes the
concurrency safe.

- **Every slot's candidate is its own real LLM call.** There is no rule-based
  candidate generator. Each call sees what earlier slots in the same round
  already proposed, so a round diversifies instead of converging. A bad or
  unparseable reply just drops that one slot; a round only ends the iteration
  if *every* slot's call comes back empty.
- **Duplicate and no-op candidates are rejected in code, not by the prompt.**
  A slot proposing a change another slot is already testing, or proposing a
  value a knob already holds, costs a bounded retry with the reason fed back
  to the model. Both were observed live; the prompt alone does not prevent
  them.
- Slots are **correlated after every round**, not merely run side by side:
  the round is rendered back as the controlled experiment it is (one starting
  config, each slot's delta from it), a referee turn reads all N results
  together and writes the note every slot sees next round, and the ledger
  accumulates each knob's measured effect across all runs and slots.
- **Width is capped by `hello_orfs.num_workers`** in `cluster.yaml` (ships as
  `6`). Asking for a wider `--batch-size` than that just queues the extra
  slots. Raise it only after checking host headroom with `docker stats` /
  `free -m` during a real round -- measured on a 24-core/30GB host with `gcd`,
  each concurrent run took ~1 core and ~1GB, so 6-wide fits comfortably. That
  does **not** transfer to a large design; `riscv32i`'s route stage is far
  heavier.
- **Wall-clock does not improve proportionally with width**, because the
  decision phase is sequential -- one LLM call per slot, in series, so each
  can see the earlier ones. Measured here: median LLM call 62s, referee 41s,
  flow round ~90s regardless of width. So a round costs roughly
  `62 * batch_size + 41 + 90` seconds: 255s at width 2, but ~500s at width 6,
  where most of the round is spent deciding rather than running flows.
  Widening buys more experiments per round, not proportionally faster rounds.

## Locking tunables the LLM can never change

Some knobs you don't want touched -- a utilization ceiling dictated by a
padframe, a routing layer budget, whatever. Pass `--lock` (repeatable):

```bash
python3 orfs_loop.py ... \
  --lock CORE_UTILIZATION=38 \
  --lock MAX_ROUTING_LAYER=met5 \
  --lock CLOCK_PERIOD=1.1
```

## Every run starts from a clean config

Each run resets the design's `config.chia.mk` back to its pristine
`config.mk` baseline before starting. Without that, a new run silently
inherits whatever tunables the *previous* run happened to leave behind, so
its "baseline" measurement is really some half-explored config -- results
aren't reproducible and comparing two runs means nothing.

Pass `--resume-config` (or tick *Resume previous config* in the dashboard)
to deliberately continue where a previous run stopped.

> **Note on the clock target.** `CLOCK_PERIOD` *is* a tunable, but it isn't
> a config.mk key -- ORFS has no such variable. Every design's
> `constraint.sdc` hardcodes its own `clk_period` (as `set clk_period <N>`
> or a literal `-period <N>` inside `create_clock`, depending on the
> design), so this repo patches a working copy of that file directly and
> points `SDC_FILE` at it, instead of writing an env var nothing reads.
> Units follow whatever that design's own SDC already uses (ns for most
> PDKs here, ps for asap7/gf180) -- the prompt always shows the current
> value first. Bounds are computed per-design at startup (roughly 0.3x-3x
> the design's original period), since a sane period for one PDK is
> nonsense for another. `--lock CLOCK_PERIOD=<value>` still works normally.

This is enforced **in code**, not just by asking nicely in the prompt:
- The locked value is baked into the working config immediately at startup,
  even if it disagrees with the baseline.
- Every proposed tunable diff goes through `apply_tunables_remote`, which
  forces locked keys back to their fixed value regardless of what was
  requested and reports back which keys it rejected into the trace log, so
  a curious reader can see it -- but the enforcement doesn't depend on the
  model reading that; the config file is always correct.
- Only keys already in `orfs_tunables_schema.json` can be locked (same
  whitelist the LLM itself is bound by).

### Reverting a tunable to baseline

The model's JSON reply can use `null` as a key's value, to remove its
override entirely, e.g. `{"CORE_UTILIZATION": null}` -- the key drops out of
the managed block and that setting falls back to whatever the design's own
baseline `config.mk` (or, for `CLOCK_PERIOD`, its `constraint.sdc`)
originally had. This is different from picking some new in-range value:
it's "go back to untouched," for when a change didn't help and the honest
move is to undo it rather than guess again. A locked key can't be unset
this way either -- same enforcement path as any other rejected change.

## Onboarding a new design

If your RTL doesn't have an ORFS `config.mk` yet (e.g. a fresh Tiny Tapeout
repo), generate one first:

```bash
git clone https://github.com/<you>/<your-tt-repo>.git ~/Desktop/<your-tt-repo>

python3 orfs_onboard.py \
  --design-repo ~/Desktop/<your-tt-repo> \
  --output-root ./generated-flow-configs \
  --container-flow-dir /root/OpenROAD-flow-scripts/flow \
  --platform sky130hd \
  --docker-container chia-orfs-<your-username>-0
```

This copies the RTL + a generated `config.mk`/`constraint.sdc` into
`generated-flow-configs/designs/sky130hd/<design>/` on the host **and** into
`orfs-native-build/flow/designs/sky130hd/<design>/` inside the container
(because `--docker-container` was given) -- that second location, under
`orfs-native-build/flow/designs/...`, is the one you point `orfs_loop.py` at.
The host copy under `generated-flow-configs/` is just a staging area; ORFS
never reads it directly.

Run the dry-run `make synth` the script prints at the end before trusting the
closure loop on top of it -- ORFS variable names drift across releases, and
this catches a bad config before burning an LLM turn on it.

## What you get back

Every run writes to `--run-dir` (default `orfs_runs/<unix-ts>`):

```
orfs_runs/<run-id>/
  summary.json            final status: closed / no_closure / max_iterations_reached,
                           plus per-iteration records and the achieved objective value
  tool_trace.log          the single place to see exactly what happened, in order:
                           CALL:/RETURN: pairs for each flow run, the model's raw
                           "SYSTEM: Iteration N LLM Reasoning" replies, CALL:/RETURN:
                           for each tunable diff applied, "SYSTEM: Starting Iteration
                           N" markers, and the final signoff result
  final.gds                the best run's GDS
  final_layout.webp        the best run's layout preview
  gds/run_N_final.gds      one per flow run (a run is one flow execution + one LLM turn)
  gds/run_N_layout.webp
  summaries/run_N.json     the parsed closure summary for that run
  latest_summary.json      = summaries/<highest N>.json
  ledger.json              every tunable change's measured effect, accumulated
                           across all runs/slots -- reusable via --seed-ledger
  signoff_logs/            drc.log / lvs.log plus the report artifacts
                           themselves (6_drc.lyrdb, 6_drc_count.rpt,
                           6_lvs.lvsdb, 6_lvs.log) -- only if signoff ran
  reports/run_N/           ORFS's own report directory, copied out per run:
                           per-stage .rpt files, 6_finish.rpt, metadata.json,
                           the DRC database, and ORFS's rendered webps
                           (congestion, IR drop, routing, worst path, clocks)
  flow_logs/run_N/
    synth.log, floorplan.log, place.log, cts.log, route.log,
    finish.log, metadata.log      full stdout+stderr per stage (not the
                                   truncated tail the LLM sees)
  driver.log               only present for GUI-launched runs -- the GUI's own
                           subprocess stdout/stderr capture
```

Copying `reports/run_N/` out is not optional bookkeeping: every flow run
starts with `make clean_all`, so those files exist in ORFS's own tree only
until the next execution of that variant. They are archived on **crashed**
runs too, which is the case they matter most for -- a crash produces no
`metadata.json` to parse, so the partial reports are the only quantitative
record of how far the flow got. This is a different thing from
`flow_logs/run_N/`, which is `make`'s stdout: the tools' narration, not the
reports they produced.

Files the container writes (everything except `summary.json` and the first
line of `tool_trace.log`) are root-owned -- readable by you, but not
writable/deletable directly. To remove a run folder:

```bash
docker exec chia-orfs-<your-username>-0 rm -rf /root/chia-orfs/orfs_runs/<run-id>
```

## Signoff (DRC / LVS)

After a successful close the driver runs standalone KLayout `make drc` /
`make lvs` on the **best** run -- not the last one tried, which is usually a
different run. Skip it with `--skip-signoff`.

Signoff targets the tree that actually holds the best run's GDS: in batch mode
that's the winner's own `slotN` tree, with no rebuild at all, so the verdict is
exact by construction rather than depending on a rebuild reproducing the same
layout. As a cross-check the driver md5s the promoted `final.gds` and
`run_signoff_remote` md5s the GDS it actually read, reporting
`signed_off_gds{path, md5, expected_md5, matches_best}` into `summary.json`
plus a warning on mismatch. A verdict for the wrong layout reads exactly like a
real one, which is why that check exists.

**On sky130hd, read `result["lvs_verdict"]`, not `result["lvs"]["clean"]`.**
The former is netgen's answer and is authoritative; the latter is KLayout's.
KLayout's own combiner only ever does *parallel* MOS combination with no series
step, so it fails on cells whose layout folds devices differently from the
schematic. `lvs/sky130hd_netgen_setup.tcl` ports open_pdks' netgen setup (the
one OpenLane and Tiny Tapeout use) and runs as a second opinion alongside
KLayout -- it needs no magic and no sky130A PDK, because it reads the
`*_extracted.cir` KLayout already wrote. We borrow the comparator, not the
extractor. On `riscv32i` that made device counts match exactly (5376 = 5376)
where KLayout could not.

One cell, `sky130_fd_sc_hd__a21oi_2`, is a verified-benign fold mismatch that
neither comparator can close on its own, and it is gated on by exact signature
in code (`KNOWN_NETGEN_FOLD_EXCEPTIONS` in `orfs_tool.py`) rather than by a
blanket "ignore LVS" switch. Its NMOS AND-leg is laid out as two parallel
half-width series stacks rather than the one full-width pair its `m=2`
schematic collapses to -- a real, cell-intrinsic mask decision, not a missing
connectivity rule. Any *other* mismatching subcircuit keeps the gate closed.

## Choosing a model

The LLM has no tools -- it just replies to a prompt with a fenced JSON
tunable diff or a `CLOSURE:` sentinel, and the driver parses that reply.
Reliability now hinges on whether a model actually *follows that reply
format*, not on tool-calling. Closure success is still **not independently
verified** -- it trusts the model's own `CLOSURE: PASS` reply. Cross-check
`tool_trace.log`: a real run has `CALL: run_full_flow_and_summarize` entries
with real metrics; a model that's just narrating prose instead of a valid
JSON/sentinel reply leaves the loop unable to parse a proposal at all, even
if its prose claims closure.

- `google/gemini-3.5-flash` doesn't exist in opencode's catalog and a run
  against it hangs indefinitely rather than failing fast -- confirmed by
  hand. `google/gemini-3.6-flash` (the current default) is the real model
  one version up; if a run against some model string never produces its
  first trace entry, suspect this before assuming a cluster problem.
- `opencode/big-pickle` reliably produces valid replies in testing, at the
  cost of being slow (each turn can take several minutes, since a real flow
  run executes the whole ORFS pipeline).

## Why every flow run does a full clean rebuild

`make <stage>` decides whether to rebuild by comparing file timestamps
between a target and its tracked prerequisites (Verilog sources, previous
stage's outputs) -- it has no way to know `DESIGN_CONFIG`'s *contents*
changed, since that's an env var, not a file ORFS's Makefile tracks as a
dependency. Without an explicit clean, every call after the first would
silently no-op (`make: Nothing to be done for 'synth'.`) and just re-report
the first run's stale metrics regardless of what tunables changed -- this
was a real bug here for a while, and is why `run_flow_remote` now runs
`make clean_all` before the stage sequence on every single call.

This means each call costs a genuine full rebuild (`gcd`: ~1-2 min;
`riscv32i`: 15+ min, mostly detailed routing) rather than the ~1s a no-op
would take. That's the actual cost of a working closure loop -- if a call
ever finishes in a couple seconds, something is wrong; check `flow_logs/run_N/*.log`
for `Nothing to be done` to confirm before trusting the result.

## Troubleshooting

**A run hangs forever / `ray status` shows `orfs_run` fully used with
nothing running.** A previous run was killed uncleanly (e.g. `timeout`,
`kill -9`, or a crash before cleanup ran) and left orphaned actors holding
`orfs_run` slots, so nothing new can schedule until they're freed:

```bash
ray status   # confirms orfs_run fully used, "Pending Demands" for orfs_run
python3 -c "
import ray
ray.init(address='auto')
import ray.util.state as st
for a in st.list_actors(filters=[('state','=','ALIVE')]):
    print(a['actor_id'], a['class_name'], a['pid'])
"
# find the orphaned actor's pid, then (in whichever worker container it's in):
docker exec chia-orfs-<your-username>-0 kill -9 <pid>
```

Before assuming a hang, check whether the flow is genuinely still working:
`docker exec chia-orfs-<your-username>-0 ps -eo pid,etime,pcpu,stat,comm` --
state `R` with high CPU on an `openroad` process means it's grinding (e.g.
TritonRoute working through a DRC-convergence stall), not stuck.

**A long run dies with `Failed to connect to GCS within 60 seconds`, or `No
node info found matching node ids: set()`.** Check whether the GCS actually
died (`pgrep -af gcs_server`) before believing it -- on flaky wifi the far
more common cause is the host's real IP rotating mid-run (a DHCP lease
turning over across a suspend/reboot, say). This is unrelated to
`cluster.yaml`'s `head_ip`/`compatible_ips` being pinned to `127.0.0.1` --
that pin only covers chia's own SSH/docker orchestration. **Ray's own node
identity is a separate, lower layer that cannot be pinned to loopback**: Ray
deliberately substitutes the real outbound IP for `127.0.0.1` even when you
pass it explicitly (`ray._private.services.resolve_ip_for_localhost`), so
every node is always registered under the host's real, rotating address.
When that address changes mid-session, `ray status` keeps answering over
loopback and reports full capacity, while a real `ray.init` fails to find
any node matching the new address.

There is no way to make the running session survive this -- the fix is
recovery, not prevention: `chia up cluster.yaml -y` again. This is where the
`127.0.0.1` pin actually pays off: recovery no longer depends on knowing (or
recomputing) the host's current IP first, and it reconciles the existing
Docker containers rather than rebuilding them, so it's much faster than a
cold start. A run's in-progress state up to the crash (`summaries/`,
`reports/`, `gds/`, `ledger.json`) survives; only `summary.json` and signoff
are lost, since those are written at the very end.

**A `place` stage crashes with `[ERROR GPL-0301] Utilization exceeds 100%`.**
`CELL_PAD_IN_SITES_GLOBAL_PLACEMENT` inflates each cell's effective footprint,
so it can push placement utilization over 100% even when `CORE_UTILIZATION`
itself is well inside its legal range -- nothing cross-checks the two knobs
against each other. Observed repeatedly across platforms, at padding values as
low as `2`, so the safe threshold is design- and utilization-dependent rather
than a constant. It stays non-fatal to a run: that slot's run is archived and
marked crashed, and the round continues on the other slots.

**`FileNotFoundError` on `config.mk` or the schema at startup.** You passed a
host path instead of a container-native one, or `setup()`'s own file reads
(schema, baseline config) landed on the wrong filesystem. Re-check every path
argument is `/root/OpenROAD-flow-scripts/...` or `/root/chia-orfs/...`, never
a bare host path like `~/chia-hackathon/orfs_loop/...`.

**CTS stage fails with `kepler-formal: ... libpython3.14.so.1.0: cannot open
shared object file`.** Missing `LD_LIBRARY_PATH=/opt/miniconda/lib` in the
worker's environment -- `orfs_loop.py --ld-library-path` already defaults to
this; if you're calling the remote functions directly, pass
`extra_env={"LD_LIBRARY_PATH": "/opt/miniconda/lib"}`. The same fix causes a
cosmetic `bash: /opt/miniconda/lib/libtinfo.so.6: no version information
available` warning on every shell `make` spawns (it shadows the system
`libtinfo` with conda's build) -- harmless, not a sign of anything slow or
broken.

**Edits to `orfs_tool.py` don't seem to take effect.** Confirm the container
actually has the new mount: `docker exec chia-orfs-<your-username>-0 python3 -c
"import orfs_tool; print(orfs_tool.__file__)"` should print
`/root/chia-orfs/orfs_tool.py`. If it prints somewhere under `site-packages`
instead, the container predates the `cluster.yaml` mount fix -- recreate it:
`docker stop chia-orfs-<your-username>-0 && docker rm chia-orfs-<your-username>-0 && chia up cluster.yaml -y`
(only recreates this one container; the other worker containers are untouched).

**The GUI's model dropdown is empty, or any `docker` command fails with
"permission denied".** Your shell session predates being added to the
`docker` group -- group membership changes don't retroactively apply to an
already-running session. Prefix the command with `sg docker -c "..."` or
open a fresh terminal.

**`make lvs` fails outright on sky130hd designs** (a parse error, not a
mismatch verdict). The vendored
`orfs-native-build/flow/platforms/sky130hd/cdl/sky130hd.cdl` has two upstream
quirks KLayout's LVS netlist reader rejects: literal `short` values on 6
tie-cell resistor lines (needs a numeric `0`), and a slash-delimited
`net1 net2 ... / subcktname` format on 7 `X`-instance lines inside
`macro_sparecell` (the reader counts the `/` as a spurious extra net).
`sky130hd.lylvs` needs two fixes of its own: it called DRC's `report()`
instead of `report_lvs()` -- so it wrote no LVS database at all, and a
mismatch could not be diagnosed from any artifact the flow kept -- and it
shipped with device combination disabled.

All of these are hand-patched on this host's copy of `orfs-native-build/`.
The patch is *not* in `chia-orfs-run`'s Docker image, which is built straight
from public `openroad/orfs:latest` with nothing local baked in, so it needs
reapplying after a fresh clone or reset of that submodule.

**Even fully patched, sky130hd LVS reports a genuine `Netlists don't match`.**
Measured on `riscv32i` (5687 cells): exactly 10 circuits fail, in three
unrelated groups -- and the design's own top-level netlist is **not** among
the things actually compared:

- 7 **schematic-only** cells that are physical-only and unextractable, so the
  schematic side declares devices/pins the layout side structurally cannot
  produce. `conb_1` is the resistor case (the deck's `extract_devices` covers
  MOSFETs only); `fill_1/2/4/8`, `diode_2` and `tapvpwrvgnd_1` are the *empty*
  case -- their CDL bodies declare pins and contain no devices at all.
- `riscv` (the top circuit) comes back **`Skipped`**, not matched: once
  subcircuits fail, the top-level comparison is abandoned. A run in this state
  gives *no* LVS assurance about the design's own connectivity, in either
  direction. Don't read "only 10 mismatches" as "99.8% verified".
- 2 real standard-cell fold failures (`a21oi_2`, `ha_4`) where the same device
  classes appear on both sides but won't pair.

The failing set tracks *which cell types the design happens to instantiate*,
which is the clearest evidence this is a library/deck gap rather than a
connectivity problem -- and nothing the closure loop's knobs influence.
Measured DRC/LVS on a verified-correct best GDS: **DRC 0 violations, LVS 9
mismatches with the top circuit `Skipped`.**
