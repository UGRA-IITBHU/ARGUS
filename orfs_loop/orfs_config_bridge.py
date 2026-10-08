"""Bridge between an LLM-editable tunables.json and ORFS's native config.mk.

Why not let the LLM edit config.mk directly?
  - config.mk is a Makefile fragment. A stray `$(shell ...)` or unescaped
    value is a code-execution vector once `make` sources it.
  - It mixes structural config (verilog sources, PDK paths, pin ordering)
    with the handful of knobs actually relevant to closure. An LLM editing
    the whole file can accidentally break the design, not just retune it.

Instead: `tunables.json` is a flat dict validated against
`orfs_tunables_schema.json` (closed key whitelist, typed, ranged). This
module renders it into a clearly delimited block appended to the END of a
copy of your baseline config.mk. GNU Make resolves plain `=`/`export`
assignments in file order, so a later assignment in the managed block wins
over an earlier one in the baseline -- meaning we never need to parse or
rewrite the baseline file's existing lines at all.
"""

from __future__ import annotations

import json
import re
import shutil
from pathlib import Path
from typing import Any

MANAGED_BEGIN = "# --- ARGUS-managed tunables (auto-generated, do not hand-edit below) ---"
MANAGED_END = "# --- end ARGUS-managed tunables ---"

# Values must be safe to embed in a Makefile assignment: no $, `, ;, |, &,
# newlines, or quotes. This is deliberately conservative -- reject anything
# that isn't alnum / space / . / _ / - / + so a compromised or hallucinating
# LLM can't smuggle a `$(shell ...)` call through a tunable value.
_SAFE_VALUE_RE = re.compile(r"^[A-Za-z0-9_.+\- ]+$")

# Clock period isn't a config.mk variable in ORFS at all -- every design
# hardcodes its clk_period directly in constraint.sdc, in one of two shapes:
# `set clk_period <N>` (referenced by variable from create_clock elsewhere in
# the file) or a literal `-period <N>` inline in create_clock with no
# variable. A schema entry with `"target": "sdc"` gets patched into a working
# copy of the SDC file instead of rendered as a config.mk export.
_SDC_SET_PERIOD_RE = re.compile(r"(set\s+clk_period\s+)[0-9.]+")
_SDC_CREATE_CLOCK_PERIOD_RE = re.compile(r"(create_clock\b[^\n]*?-period\s+)[0-9.]+")


class TunableValidationError(ValueError):
    pass


def scale_die_core_area(area_str: str, scale: float) -> str:
    """Scale a DIE_AREA/CORE_AREA string ("x1 y1 x2 y2", microns) uniformly
    around the origin by `scale`. Both rectangles are scaled by the same
    factor so the core-to-die margin stays proportional rather than the
    core potentially ending up outside the die."""
    parts = [float(x) * scale for x in area_str.split()]
    return " ".join(str(int(p)) if p == int(p) else f"{p:.4f}".rstrip("0").rstrip(".") for p in parts)


def patch_clock_period(sdc_text: str, value: str) -> str:
    """Return sdc_text with its clock period replaced by `value`. Tries the
    `set clk_period` pattern first, falls back to a literal `-period` inside
    create_clock (all matches, in case of multiple independent clocks)."""
    if _SDC_SET_PERIOD_RE.search(sdc_text):
        return _SDC_SET_PERIOD_RE.sub(rf"\g<1>{value}", sdc_text, count=1)
    if _SDC_CREATE_CLOCK_PERIOD_RE.search(sdc_text):
        return _SDC_CREATE_CLOCK_PERIOD_RE.sub(rf"\g<1>{value}", sdc_text)
    raise TunableValidationError(
        "Could not find a clk_period assignment to patch in this design's constraint.sdc"
    )


def load_schema(schema_path: str | Path) -> dict[str, Any]:
    with open(schema_path) as f:
        schema = json.load(f)
    schema.pop("_comment", None)
    return schema


def validate_tunables(tunables: dict[str, Any], schema: dict[str, Any]) -> dict[str, str]:
    """Validate `tunables` against `schema`. Returns a dict of {key: rendered_str}.

    Raises TunableValidationError on any unknown key, out-of-range value, or
    value containing characters unsafe to embed in a Makefile assignment.
    """
    rendered: dict[str, str] = {}
    unknown = set(tunables) - set(schema)
    if unknown:
        raise TunableValidationError(
            f"Unknown tunable(s) not in schema whitelist: {sorted(unknown)}. "
            f"Allowed: {sorted(schema)}"
        )

    for key, spec in schema.items():
        if key not in tunables:
            continue
        value = tunables[key]
        kind = spec["type"]

        if kind == "float":
            fval = float(value)
            lo, hi = spec.get("min"), spec.get("max")
            if lo is not None and fval < lo or hi is not None and fval > hi:
                raise TunableValidationError(f"{key}={fval} outside [{lo}, {hi}]")
            rendered[key] = repr(fval) if fval != int(fval) else str(int(fval))

        elif kind == "int":
            ival = int(value)
            lo, hi = spec.get("min"), spec.get("max")
            if lo is not None and ival < lo or hi is not None and ival > hi:
                raise TunableValidationError(f"{key}={ival} outside [{lo}, {hi}]")
            rendered[key] = str(ival)

        elif kind == "enum":
            sval = str(value)
            if sval not in spec["values"]:
                raise TunableValidationError(f"{key}={sval!r} not in {spec['values']}")
            rendered[key] = sval

        else:
            raise TunableValidationError(f"Unknown schema type {kind!r} for {key}")

        if not _SAFE_VALUE_RE.match(rendered[key]):
            # Should be unreachable given the type checks above, but this is
            # the actual safety boundary -- keep it as a hard backstop.
            raise TunableValidationError(f"{key}: rendered value unsafe to embed: {rendered[key]!r}")

    return rendered


def load_current_tunables_from_config_mk(config_mk_path: str | Path, schema: dict[str, Any]) -> dict[str, str]:
    """Best-effort read of current values for schema keys out of an existing
    config.mk (baseline or managed block, whichever assigns last wins, matching
    make's own resolution order). Returns only keys actually found."""
    text = Path(config_mk_path).read_text()
    found: dict[str, str] = {}
    # Matches: [export] KEY [?:+]= value   (value up to end of line, comment-stripped)
    line_re = re.compile(r"^\s*(?:export\s+)?([A-Z0-9_]+)\s*[:+?]?=\s*(.+?)\s*(?:#.*)?$")
    for line in text.splitlines():
        m = line_re.match(line)
        if not m:
            continue
        key, value = m.group(1), m.group(2).strip()
        if key in schema:
            found[key] = value  # later lines overwrite earlier -> last wins, matches make
    return found


def render_config_mk(
    baseline_config_mk_path: str | Path,
    tunables: dict[str, Any],
    out_path: str | Path,
    schema: dict[str, Any],
) -> dict[str, str]:
    """Write out_path = baseline config.mk + a managed override block for
    `tunables` (validated against `schema`). Returns the rendered {key: value}
    dict actually written. Idempotent: re-running with the same tunables
    replaces the previous managed block rather than appending duplicates."""
    rendered = validate_tunables(tunables, schema)

    baseline_text = Path(baseline_config_mk_path).read_text()
    # Strip any previous managed block so re-renders don't accumulate stale
    # blocks (which would still "win" under make's last-assignment rule, but
    # would make the file confusing to read).
    pattern = re.compile(
        re.escape(MANAGED_BEGIN) + r".*?" + re.escape(MANAGED_END) + r"\n?",
        re.DOTALL,
    )
    baseline_text = pattern.sub("", baseline_text).rstrip() + "\n"

    block_lines = [MANAGED_BEGIN]
    for key, value in sorted(rendered.items()):
        block_lines.append(f'export {key} = {value}')

    # Keys targeting the SDC file (currently just CLOCK_PERIOD) still get
    # written above as a plain export -- inert to make, but that's how their
    # value round-trips through load_current_tunables_from_config_mk on the
    # next call, same as every other tunable. The actual effect is a patched
    # copy of constraint.sdc plus an SDC_FILE override here that points at it.
    sdc_keys = {k: v for k, v in rendered.items() if schema[k].get("target") == "sdc"}
    if sdc_keys:
        baseline_sdc_path = Path(baseline_config_mk_path).parent / "constraint.sdc"
        # Derived from out_path's own name (not just its parent directory),
        # so distinct working_config_mk files in the SAME directory (e.g.
        # orfs_loop.py's --batch-size slots: config.chia.slot0.mk,
        # config.chia.slot1.mk, ...) get distinct patched SDC files instead
        # of all colliding on one shared "constraint.chia.sdc" -- that
        # collision was invisible at batch_size=1 (only one writer/reader
        # ever existed at a time) but would silently corrupt CLOCK_PERIOD
        # across concurrent slots otherwise. "config.chia.mk" ->
        # "constraint.chia.sdc" preserves today's exact filename.
        out_stem = Path(out_path).stem
        sdc_stem = out_stem.replace("config", "constraint", 1) if out_stem.startswith("config") else f"constraint.{out_stem}"
        working_sdc_path = Path(out_path).parent / f"{sdc_stem}.sdc"
        sdc_text = baseline_sdc_path.read_text()
        for key, value in sdc_keys.items():
            if key == "CLOCK_PERIOD":
                sdc_text = patch_clock_period(sdc_text, value)
        working_sdc_path.write_text(sdc_text)
        block_lines.append(f"export SDC_FILE = {working_sdc_path}")

    # DIE_AREA_SCALE (only ever offered -- see ORFSTool.setup -- for designs
    # whose baseline already uses ORFS's explicit die/core-area floorplan
    # method) scales both rectangles by the same factor and overrides them
    # directly; this is the one tunable that maps onto two config.mk keys
    # at once rather than one SDC line, but otherwise follows the same
    # "read baseline, transform, override" shape as CLOCK_PERIOD above.
    if "DIE_AREA_SCALE" in rendered:
        line_re = re.compile(r"^\s*(?:export\s+)?([A-Z0-9_]+)\s*[:+?]?=\s*(.+?)\s*(?:#.*)?$")
        baseline_areas: dict[str, str] = {}
        for line in Path(baseline_config_mk_path).read_text().splitlines():
            m = line_re.match(line)
            if m and m.group(1) in ("DIE_AREA", "CORE_AREA"):
                baseline_areas[m.group(1)] = m.group(2).strip()
        scale = float(rendered["DIE_AREA_SCALE"])
        for key in ("DIE_AREA", "CORE_AREA"):
            if key in baseline_areas:
                block_lines.append(f"export {key} = {scale_die_core_area(baseline_areas[key], scale)}")

    block_lines.append(MANAGED_END)

    out_text = baseline_text + "\n" + "\n".join(block_lines) + "\n"
    Path(out_path).write_text(out_text)
    return rendered


def backup_config_mk(config_mk_path: str | Path, backup_dir: str | Path) -> Path:
    """Copy config_mk_path into backup_dir with a timestamp suffix, before
    the loop's first write, so the exact pre-agent baseline is always
    recoverable regardless of what the LLM does afterward."""
    import time

    backup_dir = Path(backup_dir)
    backup_dir.mkdir(parents=True, exist_ok=True)
    dest = backup_dir / f"config.mk.baseline.{int(time.time())}"
    shutil.copy2(config_mk_path, dest)
    return dest
