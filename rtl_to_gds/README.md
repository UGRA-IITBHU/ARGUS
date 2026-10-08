# rtl_to_gds — UVM verification → ORFS closure

Part of [ARGUS](../README.md) — Agentic RTL-to-GDS Using UVM Sign-off. This is
the orchestrator that chains the two loops into one run.

`rtl_to_gds.py` chains the two loops into one run:

1. **UVM stage.** Runs `uvm_loop`'s `pipeline.run14` on a design YAML. The loop builds a testbench and repairs the RTL until it is verified.
2. **Handoff.** Reads `generated/designs/<design>/rtl_verification/rtl_verification_state.json`. Only RTL marked `verified` (or `complete` with `rtl_outcome: no_repair`) is passed on, unless you give `--allow-unverified`. The RTL passed on is the accepted snapshot in `rtl_verification/accepted/`, never the original benchmark file.
3. **ORFS onboarding.** Writes `config.mk`, `constraint.sdc`, the RTL and `rtl_handoff.json` (provenance, including the RTL's md5) to `$CHIA_ORFS_REPO/rtl_to_gds_designs/<platform>/chia_<design>/`. The top module and clock port come from the UVM loop's `rtl_info.json`. Designs with no clock get a virtual-clock SDC. `.sv` files are synthesized with the slang frontend.
4. **ORFS stage.** Runs `orfs_loop.py` on that design. The final GDS is written to `orfs_runs/chia_<design>-<ts>/final.gds`, along with `rtl_to_gds.json`, a record linking the GDS back to the verified RTL.
5. **Run folder.** Everything the run produced is gathered into one
   self-contained directory, `runs/<design>/<timestamp>/`, so nothing has to be
   hunted down across `uvm_loop/generated/` and `<orfs-repo>/orfs_runs/`
   afterwards:

   ```text
   runs/<design>/<timestamp>/
   ├── uvm/            # rtl_verification/, results/, checkpoints/*.done
   ├── orfs/           # flow_logs/, signoff_logs/, reports/, summary.json
   ├── orfs_config/    # config.mk, constraint.sdc, rtl_handoff.json, the RTL
   ├── console.log     # full stdout/stderr of BOTH stages, teed live
   ├── manifest.json   # what was collected, from where, and the final GDS path
   └── rtl_to_gds.json # the run record
   ```

   `orfs_config/` is the config the run actually closed with — the one thing
   you need to reproduce the result. Originals are left in place; this is a
   copy, not a move. Physical-design artifacts (GDS/DEF/LEF/ODB) are *not*
   copied — they stay in the ORFS run dir, which `manifest.json` points at.

   Change the parent with `--runs-dir`, pin one exact directory with
   `--logs-dir`, or turn it off with `--no-logs-collect`.

## Cluster

Both stages need their workers up at the same time, so use the combined `cluster.yaml` in this directory:

```bash
conda activate chia_env
export CHIA_PROJECT_ROOT=$PWD/uvm_loop
export CHIA_ORFS_REPO=$PWD/orfs_loop      # or wherever orfs-native-build/ is checked out
chia down <old cluster.yaml> -y           # whichever per-loop cluster is running
docker rm -f chia-opencode-$USER-0 chia-rtl-$USER-0 chia-sim-$USER-0   # once: old mounts
chia up rtl_to_gds/cluster.yaml -y
```

Before starting any work, the script checks that the needed Ray resources exist and that each container mounts the directories this run uses.

## Run

```bash
python3 rtl_to_gds/rtl_to_gds.py --design-config benchmarks/fifo/design.yaml --clock-period 10 \
    -- --max-iterations 3 --objective area --model opencode/big-pickle --stage-timeout-seconds 7200
```

Arguments after `--` are passed straight to `orfs_loop.py`.

| Flag | Purpose |
|---|---|
| `--skip-uvm` | Use an existing verified result instead of re-running the UVM loop |
| `--uvm-supervised N` | Run the UVM stage under `run_forever.sh` (auto-restart, N improvement iterations) |
| `--allow-unverified` | Send unverified RTL to ORFS anyway |
| `--prepare-only` | Stop after writing the ORFS design and print the `orfs_loop.py` command |
| `--top`, `--clock-port` (`none` = combinational) | Override auto-detection |
| `--clock-period` | Starting period in the platform's SDC units (ns on sky130hd); the ORFS loop can tune it |
| `--core-utilization`, `--place-density` | Starting floorplan values (default 40 / 0.6) |
| `--synth-frontend auto\|slang\|yosys` | HDL reader for synthesis |
| `--orfs-repo` | ORFS repo the `orfs_run` workers mount (default `$CHIA_ORFS_REPO`, else `orfs_loop/`) |
| `--runs-dir` | Parent for run folders (default: `<repo>/runs`). Each run gets `<runs-dir>/<design>/<timestamp>/` |
| `--logs-dir` | Use this exact directory as the run folder instead of the `<design>/<timestamp>` layout |
| `--no-logs-collect` | Skip the run folder and the console tee entirely |

**Tiny designs** (tens of cells, e.g. `hamming_encoder`) fail PDN at the default 40% utilization with `PDN-0185 Insufficient width`, because the die is narrower than the met4 strap pitch. Start them at `--core-utilization 10`.
