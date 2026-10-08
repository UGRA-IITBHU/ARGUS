from pathlib import Path
import ast
import json
import sys
import subprocess
import re
import time
import shutil
import os
import hashlib

import ray

import yaml


from ray.exceptions import GetTimeoutError
from chia.base.ChiaFunction import ChiaFunction, get

from chia.base.tools.BashTool import BashTool

from chia.models.opencode import OpenCodeLLM

from pipeline.functions import extract_rtl, generate_uvm_in_worker, simulate, analyze_results
from pipeline.tb_feedback import build_diagnosis_prompt, build_repair_prompt
from pipeline.pyuvm_reference import pyuvm_api_reference
from pipeline.verification_improvement import (
    analyze_verification_state,
    prepare_sanitized_workspace,
    collect_tb_changes,
    build_improvement_prompt,
    build_improvement_decision_prompt,
    save_improvement_decision,
    load_improvement_history,
    append_improvement_history,
)
from src.schema import VerificationPlan



# =========================================================

# Workspace

# =========================================================

HOST_WORKSPACE = Path(__file__).resolve().parent.parent

CONTAINER_WORKSPACE = "/workspace"


LLM_MODELS_FILE = HOST_WORKSPACE / "config" / "llm_models.txt"
LLM_MODEL_STATE = HOST_WORKSPACE / "generated" / "llm_model_state.json"
LLM_MODEL_LOG = HOST_WORKSPACE / "generated" / "llm_model_usage.jsonl"
LLM_RATE_LIMIT_COOLDOWN_S = int(os.environ.get("LLM_RATE_LIMIT_COOLDOWN_S", "3600"))
# Transient provider faults (a 500, an overload) recover in seconds, so parking
# the model for the full hour above costs far more than the fault does --
# especially for a paid model the operator put first on purpose.
LLM_TRANSIENT_COOLDOWN_S = int(os.environ.get("LLM_TRANSIENT_COOLDOWN_S", "180"))
# Wall-clock ceiling for a single LLM call. chia's get() blocks forever if the
# remote OpenCodeLLM actor deadlocks (observed live: the agent emits
# step_finish/reason=tool-calls and the tool round-trip never returns), so no
# exception is ever raised and the model-fallback path never engages.
LLM_CALL_TIMEOUT_S = int(os.environ.get("LLM_CALL_TIMEOUT_S", "1800"))
# While a call is in flight, how often to ask opencode's own log whether the
# provider has already failed, and how long to leave it alone first. The grace
# period exists so a model that is merely slow to produce its first token is
# never mistaken for a dead one. Set LLM_FAILFAST_POLL_S=0 to disable the
# early-abort check and fall back to the plain ceiling.
LLM_FAILFAST_POLL_S = int(os.environ.get("LLM_FAILFAST_POLL_S", "20"))
LLM_FAILFAST_GRACE_S = int(os.environ.get("LLM_FAILFAST_GRACE_S", "60"))
_SELECTED_LLM_MODEL: str | None = None


def _llm_model_candidates() -> list[str]:
    if LLM_MODELS_FILE.exists():
        models = [
            line.split("#", 1)[0].strip()
            for line in LLM_MODELS_FILE.read_text().splitlines()
        ]
        models = [m for m in models if m]
        if models:
            return models
    return [os.environ.get("LLM_MODEL", "opencode/big-pickle")]


def _load_llm_model() -> str:
    """Resolve the LLM model for this run14 process, skipping rate-limited ones."""
    # One model per process: a rate-limited run exits, the supervisor restarts it,
    # and the restart resumes from checkpoints on the next available model.
    global _SELECTED_LLM_MODEL
    if _SELECTED_LLM_MODEL:
        return _SELECTED_LLM_MODEL
    candidates = _llm_model_candidates()
    state = json.loads(LLM_MODEL_STATE.read_text()) if LLM_MODEL_STATE.exists() else {}
    cooldowns = state.get("cooldown_until", {})
    now = time.time()
    available = [m for m in candidates if cooldowns.get(m, 0) <= now]
    current = state.get("current")
    if current in available:
        chosen = current
    elif available:
        chosen = available[0]
    else:
        chosen = min(candidates, key=lambda m: cooldowns.get(m, 0))
    state["current"] = chosen
    LLM_MODEL_STATE.parent.mkdir(parents=True, exist_ok=True)
    LLM_MODEL_STATE.write_text(json.dumps(state, indent=2))
    with LLM_MODEL_LOG.open("a") as fh:
        fh.write(json.dumps({
            "time": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "event": "selected",
            "model": chosen,
            "argv": sys.argv[1:],
        }) + "\n")
    print(f"LLM model selected: {chosen}")
    _SELECTED_LLM_MODEL = chosen
    return chosen


# Provider faults that mean "this provider is briefly unwell",
# not "this provider is unusable". Observed live 2026-09-23: Google returned
# "Internal error encountered" and "experiencing high demand" within a minute
# of each other, and the flat one-hour cooldown benched both paid Gemini models
# -- the operator's explicit first preference -- for an hour over a blip, while
# the run fell back to free models nobody chose. These recover in seconds, so
# they get LLM_TRANSIENT_COOLDOWN_S instead.
#
# Match on distinctive phrases only. A bare "500"/"503" would collide with
# token counts, timestamps and ids in the same exception text.
_TRANSIENT_MARKERS = (
    "Internal error encountered",
    "experiencing high demand",
    "Service temporarily overloaded",
    "service_unavailable",
    "temporarily unavailable",
    "Overloaded",
    "ServerError",
    # Gateway errors (502/504) are the proxy in front of the model giving up,
    # not the model being unusable. Seen 4 times on 2026-09-23, each one
    # benching a model for a full hour and draining the pool to 4 of 12.
    "Gateway Timeout",
    "Gateway Time-out",
    "Bad Gateway",
)


_MODEL_UNAVAILABLE_MARKERS = (
    "RateLimitError", "Rate limit exceeded",
    # The bare HTTP reason, for providers that report a 429 without chia's
    # exception prefix -- e.g. {"status":429,"title":"Too Many Requests"}.
    "Too Many Requests",
    # Raised by llm_get() when a call exceeds LLM_CALL_TIMEOUT_S.
    "LLM call timed out",
    # Provider/model itself broken or gone, not just throttled (observed live:
    # opencode/nemotron-3-ultra-free returning a bare 404) -- retrying the
    # same model would fail identically forever, so treat it the same as a
    # rate limit: cool it down and let the next restart pick another model.
    "InvalidRequestError", "Upstream request failed", "Provider returned error",
    # See generate_candidate_plan(): a swallowed subprocess.TimeoutExpired across
    # all internal retries comes back as success=False with no exception and empty
    # result/stderr -- treat that silent stall the same as a rate limit.
    "silent timeout, empty response",
    # Observed live 2026-09-18 on nvidia4/openai/gpt-oss-20b: the model
    # repeatedly emits a malformed tool-call header ("unexpected tokens
    # remaining in message header"), burning all 5 of opencode's internal
    # retries on effectively every long/complex agentic stage (e.g. UVM
    # integration) without ever completing -- 30+ consecutive run14
    # restarts all re-selected the same model and hit the identical
    # failure, since this error text previously matched none of the
    # markers above and so never cooled the model down. Treat it the same
    # as a rate limit so a restart tries a genuinely different model.
    "unexpected tokens remaining in message header",
    # Transient faults count as model-unavailable too: without this they match
    # no marker, so _record_llm_rate_limit() returns early and the model is
    # never cooled down or switched away from. They only differ in how LONG
    # they are benched, which _record_llm_rate_limit() decides separately.
    *_TRANSIENT_MARKERS,
)

def _is_model_unavailable(exc: BaseException) -> bool:
    """True when the failure is the provider dying, not bad model output.

    The retry loops below exist to absorb a model that produced unusable YAML;
    retrying makes sense there because the next sample may be fine. A dead or
    throttled provider is the opposite: every retry fails identically, and
    catching it locally also hides it from _record_llm_rate_limit(), so the
    model is never cooled down and the supervisor never gets the non-zero exit
    it needs to switch. Observed live: one rate limit burned three 30-minute
    LLM_CALL_TIMEOUT_S windows inside candidate-plan generation while the
    fallback list sat idle with nine usable models.
    """
    text = f"{repr(exc)} {exc}"
    return any(marker in text for marker in _MODEL_UNAVAILABLE_MARKERS)


def _record_llm_rate_limit(exc: BaseException) -> None:
    text = f"{repr(exc)} {exc}"
    if not any(marker in text for marker in _MODEL_UNAVAILABLE_MARKERS):
        return
    model = _SELECTED_LLM_MODEL or _load_llm_model()
    transient = any(marker in text for marker in _TRANSIENT_MARKERS)
    cooldown = LLM_TRANSIENT_COOLDOWN_S if transient else LLM_RATE_LIMIT_COOLDOWN_S
    state = json.loads(LLM_MODEL_STATE.read_text()) if LLM_MODEL_STATE.exists() else {}
    until = time.time() + cooldown
    state.setdefault("cooldown_until", {})[model] = until
    state["current"] = None
    LLM_MODEL_STATE.write_text(json.dumps(state, indent=2))
    with LLM_MODEL_LOG.open("a") as fh:
        fh.write(json.dumps({
            "time": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "event": "rate_limited",
            "model": model,
            "kind": "transient" if transient else "persistent",
            "cooldown_until": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(until)),
        }) + "\n")
    kind = "briefly unwell" if transient else "unavailable/rate-limited"
    print(f"LLM model {model} {kind}; cooling down for {cooldown}s")


def _opencode_log_failure(since_epoch: float, model: str) -> str | None:
    """Return a provider error opencode logged after *since_epoch*, else None.

    opencode writes the real verdict to its own log within a second or two of a
    call starting, then -- for some providers -- hangs instead of exiting, so
    nothing surfaces through chia at all. Reading that log is the only way to
    learn early what the call already knows.

    Best-effort by construction: any missing docker, container or log just
    returns None and leaves the caller on its normal timeout. This must never
    turn a working call into a failure, so it only reports errors that are
    newer than the call and name the model the call is using.
    """
    stamp = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(since_epoch))
    # opencode timestamps are UTC ISO-8601 and sort lexicographically, so a
    # string compare is a correct "newer than" test without parsing.
    script = (
        'L=/home/ray/.local/share/opencode/log/opencode.log; [ -f "$L" ] || exit 0; '
        'tail -400 "$L" | grep "level=ERROR" | grep "stream error" | tail -5'
    )
    try:
        names = subprocess.run(
            ["docker", "ps", "--filter", "name=opencode", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=15,
        ).stdout.split()
    except (OSError, subprocess.SubprocessError):
        return None
    short_model = model.split("/")[-1]
    for name in names:
        try:
            out = subprocess.run(
                ["docker", "exec", name, "sh", "-c", script],
                capture_output=True, text=True, timeout=20,
            ).stdout
        except (OSError, subprocess.SubprocessError):
            continue
        for line in out.splitlines():
            ts = line.partition("timestamp=")[2][:19]
            if not ts or ts < stamp:
                continue
            if short_model and short_model not in line:
                continue
            detail = line.partition("error.error=")[2].strip('" ')[:200]
            if detail:
                return detail
            # No error.error= field. Fall back to the END of the line, not the
            # start: opencode puts timestamp/run/session metadata first and the
            # message last, so a head-truncation returns "timestamp=... run=..."
            # with no error phrase in it at all. That is not merely unhelpful --
            # _record_llm_rate_limit() classifies on this text, so a transient
            # fault whose phrase got truncated away is billed as persistent and
            # benches a healthy model for an hour instead of three minutes.
            return line.strip()[-200:]
    return None


def llm_get(ref, stage: str = "LLM call"):
    """get() an LLM ObjectRef with a wall-clock ceiling and an early abort.

    A stalled provider that never returns would otherwise block forever: the
    call raises nothing, so _record_llm_rate_limit() never sees an exception,
    the model is never cooled down, and the supervisor never gets the non-zero
    exit it needs to restart on a different model. Converting the timeout into
    a recognised failure is what makes the existing fallback chain automatic.

    The ceiling alone is expensive, though. Observed live: opencode/big-pickle
    logged "Rate limit exceeded" one second into a call and then hung, and the
    whole LLM_CALL_TIMEOUT_S window elapsed before anything moved -- three
    times in one afternoon. So while waiting, poll opencode's own log: once it
    has recorded a provider error for this model that is newer than this call,
    there is nothing left to wait for. Cancel and fail with a marker text, in
    seconds rather than half an hour.
    """
    started = time.time()
    deadline = started + LLM_CALL_TIMEOUT_S
    if LLM_FAILFAST_POLL_S <= 0:      # early abort disabled; plain ceiling only
        try:
            return get(ref, timeout=LLM_CALL_TIMEOUT_S)
        except GetTimeoutError as exc:
            raise RuntimeError(
                f"LLM call timed out after {LLM_CALL_TIMEOUT_S}s during {stage}; "
                "cooling down this model so the next restart picks another"
            ) from exc
    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            raise RuntimeError(
                f"LLM call timed out after {LLM_CALL_TIMEOUT_S}s during {stage}; "
                "cooling down this model so the next restart picks another"
            )
        try:
            return get(ref, timeout=min(LLM_FAILFAST_POLL_S, remaining))
        except GetTimeoutError:
            pass
        # A healthy call can be slow to produce its first token; only look once
        # the grace period has passed, so a slow start is never mistaken for a
        # dead provider.
        if time.time() - started < LLM_FAILFAST_GRACE_S:
            continue
        model = _SELECTED_LLM_MODEL or _load_llm_model()
        detail = _opencode_log_failure(started, model)
        if detail is None:
            continue
        try:
            ray.cancel(ref, force=True)
        except Exception:      # cancellation is best-effort; we exit regardless
            pass
        waited = int(time.time() - started)
        raise RuntimeError(
            f"Provider returned error for {model} after {waited}s during "
            f"{stage}: {detail}. Failing fast instead of waiting out the "
            f"{LLM_CALL_TIMEOUT_S}s ceiling; cooling down this model so the "
            "next restart picks another"
        )


def _raise_on_llm_failure(response, stage: str) -> None:
    """Shared success=False handler for llm.prompt.chia_remote() call sites.

    OpenCodeLLM.prompt() swallows subprocess.TimeoutExpired across all its own
    retries and returns success=False with empty result/stderr instead of
    raising (see generate_candidate_plan()). A generic error message here
    would never match _MODEL_UNAVAILABLE_MARKERS, so the model never cools
    down and every supervisor restart just re-picks the same dead model.
    """
    if response.success:
        return
    if not (response.stderr or "").strip() and not (response.result or "").strip():
        raise RuntimeError(
            f"OpenCode {stage} failed: silent timeout, empty response "
            "(provider likely unavailable)"
        )
    raise RuntimeError(f"OpenCode {stage} failed:\n{response.stderr}")



# =========================================================

# Project inputs

# =========================================================

# Design-specific paths are configured at startup from --design-config.
# The rest of the pipeline remains design-agnostic.
DESIGN_NAME = "adder"
RTL = "examples/adder/adder.sv"
SPEC = "examples/adder/spec.md"
REF_MODEL = "examples/adder/ref_model.py"
OUTPUT_PARENT = "generated/designs"
DESIGN_GENERATED_ROOT = f"{OUTPUT_PARENT}/{DESIGN_NAME}"
RTL_INFO = f"{DESIGN_GENERATED_ROOT}/rtl/rtl_info.json"
PLAN = f"{DESIGN_GENERATED_ROOT}/plans/verification_plan.yaml"
CANDIDATE_PLAN = f"{DESIGN_GENERATED_ROOT}/plans/candidate_verification_plan.yaml"
TB_DIR_REL = f"{DESIGN_GENERATED_ROOT}/tb"
TB_DIR = HOST_WORKSPACE / TB_DIR_REL
TB_TEST_MODULE = "test_top"
CONTRACT_PATH = f"{TB_DIR_REL}/CONTRACT.md"
CHECKPOINT_DIR = HOST_WORKSPACE / DESIGN_GENERATED_ROOT / "checkpoints"
FORCE_SCENARIO_MANIFEST_REGEN = True


def configure_design(config_path: str | Path):
    """Load one benchmark description and derive all design-local artifacts."""
    global DESIGN_NAME, RTL, SPEC, REF_MODEL, OUTPUT_PARENT, DESIGN_GENERATED_ROOT
    global RTL_INFO, PLAN, CANDIDATE_PLAN, TB_DIR_REL, TB_DIR, TB_TEST_MODULE, CONTRACT_PATH
    global CHECKPOINT_DIR

    path = Path(config_path)
    if not path.is_absolute():
        path = HOST_WORKSPACE / path
    path = path.resolve()
    if not path.exists():
        raise FileNotFoundError(f"Design configuration not found: {path}")

    cfg = yaml.safe_load(path.read_text())
    if not isinstance(cfg, dict):
        raise RuntimeError(f"Design configuration must be a YAML mapping: {path}")

    name = str(cfg.get("name", "")).strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]+", name):
        raise RuntimeError("Design config 'name' must contain only letters, digits, '_' or '-'.")

    for key in ("rtl", "spec", "ref_model"):
        if not str(cfg.get(key, "")).strip():
            raise RuntimeError(f"Design config is missing required field: {key}")

    # New layout: every benchmark owns one isolated generated root under a
    # configurable parent. The legacy `generated_root` key is still accepted
    # for compatibility, but new configurations should use `output_parent`.
    legacy_generated_root = cfg.get("generated_root")
    output_parent = str(
        cfg.get(
            "output_parent",
            Path(str(legacy_generated_root)).parent.as_posix()
            if legacy_generated_root else "generated/designs",
        )
    ).strip().rstrip("/")
    if not output_parent or Path(output_parent).is_absolute() or ".." in Path(output_parent).parts:
        raise RuntimeError("output_parent must be a non-empty project-relative path")

    DESIGN_NAME = name
    RTL = str(cfg["rtl"])
    SPEC = str(cfg["spec"])
    REF_MODEL = str(cfg["ref_model"])
    OUTPUT_PARENT = output_parent

    configured_root = str(legacy_generated_root).strip().rstrip("/") if legacy_generated_root else ""
    expected_root = f"{OUTPUT_PARENT}/{name}"
    if configured_root and configured_root != expected_root:
        raise RuntimeError(
            "generated_root is deprecated and must match output_parent/name "
            f"({expected_root}); got {configured_root}"
        )
    DESIGN_GENERATED_ROOT = expected_root

    RTL_INFO = f"{DESIGN_GENERATED_ROOT}/rtl/rtl_info.json"
    PLAN = f"{DESIGN_GENERATED_ROOT}/plans/verification_plan.yaml"
    CANDIDATE_PLAN = f"{DESIGN_GENERATED_ROOT}/plans/candidate_verification_plan.yaml"

    # Keep the complete generated TB inside the benchmark root. This prevents
    # a benchmark run from ever sharing generated Python, manifests or
    # simulator build state with another benchmark.
    tb_dir_rel = str(cfg.get("tb_dir", f"{DESIGN_GENERATED_ROOT}/tb")).strip().rstrip("/")
    if not tb_dir_rel or Path(tb_dir_rel).is_absolute() or ".." in Path(tb_dir_rel).parts:
        raise RuntimeError("tb_dir must be a non-empty project-relative path")
    if not (Path(tb_dir_rel) == Path(DESIGN_GENERATED_ROOT) / "tb" or str(cfg.get("tb_dir", "")).strip()):
        raise RuntimeError("tb_dir must be inside the benchmark generated_root")
    try:
        Path(tb_dir_rel).relative_to(Path(DESIGN_GENERATED_ROOT))
    except ValueError:
        raise RuntimeError("tb_dir must be inside the benchmark generated_root")
    TB_DIR_REL = tb_dir_rel
    TB_DIR = HOST_WORKSPACE / TB_DIR_REL

    # Create the benchmark root early and use it as the LLM agent working
    # directory. This prevents incidental agent-generated files from landing
    # in the project-level workspace.
    (HOST_WORKSPACE / DESIGN_GENERATED_ROOT).mkdir(parents=True, exist_ok=True)
    # The simulator adds TB_DIR itself to PYTHONPATH, so the top-level
    # cocotb module is always simply `test_top`. This keeps the generated
    # package independent of the chosen output-parent path (including names
    # containing hyphens).
    if not TB_DIR_REL:
        raise RuntimeError("tb_dir must not be empty")
    TB_TEST_MODULE = "test_top"
    CONTRACT_PATH = f"{TB_DIR_REL}/CONTRACT.md"
    CHECKPOINT_DIR = HOST_WORKSPACE / DESIGN_GENERATED_ROOT / "checkpoints"

    for rel, label in ((RTL, "RTL"), (SPEC, "specification"), (REF_MODEL, "reference model")):
        candidate = (HOST_WORKSPACE / rel).resolve()
        if HOST_WORKSPACE.resolve() not in candidate.parents:
            raise RuntimeError(f"{label} path escapes project workspace: {rel}")
        if not candidate.exists():
            raise FileNotFoundError(f"{label} not found: {candidate}")

    return cfg

# Keep repair disabled until generation + review are stable.

MAX_REPAIR_ATTEMPTS = 5
MAX_TB_REPAIR_ATTEMPTS = 5
MAX_RTL_REPAIR_ATTEMPTS = int(os.environ.get("MAX_RTL_REPAIR_ATTEMPTS", "5"))
MAX_RTL_VERIFICATION_ITERATIONS = int(os.environ.get("MAX_RTL_VERIFICATION_ITERATIONS", "10"))

# A non_actionable RTL diagnosis means that the LLM could not yet establish
# enough evidence for a grounded repair decision.  It is retryable, but only
# within the current RTL iteration.  It does NOT consume another RTL repair
# iteration and does NOT trigger score-based plateau detection.
MAX_RTL_NON_ACTIONABLE_RETRIES = int(
    os.environ.get("MAX_RTL_NON_ACTIONABLE_RETRIES", "3")
)

# LLM/OpenCode reliability settings. These are environment-configurable so
# the existing pipeline behavior can be restored without editing the file.
LLM_TIMEOUT_SECONDS = int(os.environ.get("LLM_TIMEOUT_SECONDS", "2400"))
LLM_RETRIES = int(os.environ.get("LLM_RETRIES", "5"))
BASH_AGENT_TIMEOUT_SECONDS = int(os.environ.get("BASH_AGENT_TIMEOUT_SECONDS", "2400"))
MAX_IMPROVEMENT_ITERATIONS = int(os.environ.get("MAX_VERIFICATION_IMPROVEMENT_ITERATIONS", "20"))
PLATEAU_PATIENCE = int(os.environ.get("TB_IMPROVEMENT_PLATEAU_PATIENCE", "5"))
PLATEAU_MIN_DELTA = float(os.environ.get("TB_IMPROVEMENT_MIN_DELTA", "1.0"))
SIM_TEST_TIMEOUT = int(os.environ.get("SIM_TEST_TIMEOUT", "60"))
VERIFICATION_REASONING_GUIDANCE = (
    HOST_WORKSPACE / "pipeline" / "verification_reasoning_guidance.yaml"
)
SIM_BUILD_TIMEOUT = 1800



# =========================================================
# Checkpoints
# =========================================================
#
# Stage 1 (RTL extraction) and Stage 2/3 (plan generation + review) each
# have a single well-defined, fixed-path output artifact (rtl_info.json,
# candidate_verification_plan.yaml, verification_plan.yaml) so resuming
# those stages is just "does that file already exist on disk".
#
# Stage 4 (staged UVM generation) is different: each of its six
# sub-stages writes LLM-authored files under the benchmark-local TB directory whose names
# aren't fixed in advance. Checkpoint markers are used for precise
# sub-stage resumption during an in-progress generation, but a complete
# the benchmark-local TB is also treated as persistent Stage-4 state. This means
# a later run will NEVER regenerate a complete existing environment merely
# because its checkpoint markers are absent.
#
# Same for Stage 5 (closed-loop
# simulation) — "did this already pass" isn't visible from a file
# listing either. For those we drop an explicit marker file the moment
# a stage completes successfully, and check for it before re-running
# that stage on a later invocation.
#
# To force a full re-run of a given stage, just delete its marker file
# (or the whole benchmark generated/checkpoints directory to start clean).
# =========================================================

def checkpoint_path(name):
    return CHECKPOINT_DIR / f"{name}.done"


def is_stage_done(name):
    return checkpoint_path(name).exists()


def mark_stage_done(name, detail=None):
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "stage": name,
        "completed_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    if detail is not None:
        payload["detail"] = detail
    checkpoint_path(name).write_text(json.dumps(payload, indent=2))


def clear_stage(name):
    """Force a stage to re-run on the next invocation."""
    path = checkpoint_path(name)
    if path.exists():
        path.unlink()


def _tb_content_hash(tb_dir: str | Path) -> str:
    """Hash generated TB source/configuration while ignoring runtime artifacts."""
    root = Path(tb_dir).resolve()
    if not root.is_dir():
        return ""
    digest = hashlib.sha256()
    excluded_dirs = {".chia_sim", "__pycache__", ".git"}
    excluded_suffixes = {".so", ".o", ".a", ".d"}
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        if any(part in excluded_dirs for part in path.parts):
            continue
        if path.suffix in excluded_suffixes or path.name.endswith(".pyc"):
            continue
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _tb_checkpoint_is_valid() -> bool:
    """Accept tb_validated only when it certifies the exact current TB."""
    path = checkpoint_path("tb_validated")
    if not path.is_file():
        return False
    try:
        payload = json.loads(path.read_text())
        recorded = str((payload.get("detail") or {}).get("tb_content_hash", "")).strip()
    except Exception as exc:
        print(f"\n⚠ Ignoring malformed tb_validated checkpoint: {exc}")
        return False
    current = _tb_content_hash(TB_DIR)
    if not recorded or recorded != current:
        print("\n⚠ tb_validated checkpoint is stale: generated TB content changed.")
        return False
    return True


def _validate_generated_tb_static(tb_dir: str | Path) -> tuple[bool, list[str]]:
    """Deterministic pre/post-repair validation for the generated cocotb TB."""
    root = Path(tb_dir).resolve()
    errors: list[str] = []
    if not root.is_dir():
        return False, [f"Generated TB directory not found: {root}"]

    python_files = sorted(
        p for p in root.rglob("*.py") if "__pycache__" not in p.parts
    )
    if not python_files:
        errors.append("Generated TB contains no Python source files.")

    for path in python_files:
        try:
            compile(path.read_text(encoding="utf-8"), str(path), "exec")
        except (SyntaxError, UnicodeDecodeError, OSError) as exc:
            errors.append(f"Python validation failed for {path.relative_to(root)}: {exc}")

    valid, manifest_errors = validate_scenario_manifest(PLAN, root)
    if not valid:
        errors.extend(f"Scenario manifest: {e}" for e in manifest_errors)
    return not errors, errors


def _verification_integrity_snapshot(tb_dir: str | Path) -> dict:
    """Count protected verification-intent constructs without executing TB code."""
    root = Path(tb_dir).resolve()
    snapshot = {
        "files": 0,
        "classes": 0,
        "test_classes": 0,
        "scenario_ids": 0,
        "assertions": 0,
        "scoreboard_checks": 0,
        "reference_model_calls": 0,
        "coverage_constructs": 0,
        "coverage_bins": 0,
        "directed_tests": 0,
    }
    patterns = {
        "assertions": re.compile(r"\bassert\b|assertion|check(?:_|[A-Z])"),
        "scoreboard_checks": re.compile(r"scoreboard|mismatch|compare\w*|expected\b|observed\b"),
        "reference_model_calls": re.compile(r"ref(?:erence)?_model|reference_model|predict\w*|model\("),
        "coverage_constructs": re.compile(r"\bcoverage\b|\bCover(Point|Cross|Group)\b|coverage_db"),
        "coverage_bins": re.compile(r"\bbins?\b|CoverPoint|CoverCross"),
        "scenario_ids": re.compile(r"\bSCENARIO_ID\b"),
        "directed_tests": re.compile(r"\bclass\s+\w*Test\b|\bclass\s+\w*Sequence\b"),
    }
    for path in sorted(root.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        try:
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=str(path))
        except Exception:
            continue
        snapshot["files"] += 1
        snapshot["classes"] += sum(isinstance(n, ast.ClassDef) for n in ast.walk(tree))
        snapshot["test_classes"] += sum(
            isinstance(n, ast.ClassDef) and "Test" in n.name for n in ast.walk(tree)
        )
        for key, pattern in patterns.items():
            snapshot[key] += len(pattern.findall(source))
    return snapshot


def _validate_verification_integrity(before: dict, after: dict, changed_files: list[str]) -> tuple[bool, list[str]]:
    """Reject repairs that silently reduce the existing verification intent."""
    errors: list[str] = []
    protected = (
        "classes", "test_classes", "scenario_ids", "assertions",
        "scoreboard_checks", "reference_model_calls", "coverage_constructs",
        "coverage_bins", "directed_tests",
    )
    for key in protected:
        old, new = int(before.get(key, 0)), int(after.get(key, 0))
        if new < old:
            errors.append(f"{key} decreased from {old} to {new}")
    for changed in changed_files:
        p = Path(changed)
        if p.is_absolute() or ".." in p.parts:
            errors.append(f"Unsafe repair path outside generated TB: {changed}")
    return not errors, errors


def _load_json_list_history(path: Path) -> list:
    """Generic reader for a JSON-array history file. Missing/corrupt -> []."""
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return []
    return data if isinstance(data, list) else []


def _append_json_list_history(path: Path, entry: dict) -> list:
    """Generic appender for a JSON-array history file, written atomically."""
    history = _load_json_list_history(path)
    history.append(entry)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(history, indent=2, sort_keys=True))
    os.replace(tmp, path)
    return history


def _format_prior_tb_attempts_context(current_attempt: int) -> str:
    """Summarize earlier same-design TB-diagnosis/repair attempts for the
    next diagnosis prompt, read directly from the tb_update_plan_iteration_*.yaml
    artifacts each attempt already writes (no separate history file needed).

    Without this, each diagnosis attempt only sees the current simulation
    result and can re-derive a diagnosis/fix that an earlier attempt already
    tried and that did not resolve the failure -- the same cross-iteration
    blindness fixed for the RTL-repair loop (see
    format_prior_iterations_context below).
    """
    results_dir = HOST_WORKSPACE / DESIGN_GENERATED_ROOT / "results"
    lines = []
    for i in range(current_attempt):
        plan_path = results_dir / f"tb_update_plan_iteration_{i}.yaml"
        if not plan_path.exists():
            continue
        try:
            prior_plan = yaml.safe_load(plan_path.read_text()) or {}
        except yaml.YAMLError:
            continue
        if not isinstance(prior_plan, dict):
            continue
        verdict = prior_plan.get("verdict", "unknown")
        root_cause = str((prior_plan.get("root_cause") or {}).get("summary", "")).strip()
        changed_files = sorted({
            str(c.get("file")) for c in (prior_plan.get("changes") or [])
            if isinstance(c, dict) and c.get("file")
        })
        suffix = f" [changed: {', '.join(changed_files)}]" if changed_files else ""
        lines.append(f"- Attempt {i} ({verdict}): {root_cause or '(no root_cause recorded)'}{suffix}")

    if not lines:
        return ""

    return (
        "PRIOR TB-DIAGNOSIS ATTEMPTS FOR THIS SAME GENERATED TB (the "
        "simulation still failed after each of these):\n" + "\n".join(lines) + "\n\n"
        "These are provided so you do not repeat a diagnosis or fix that "
        "already failed to resolve the failure. Before citing any file or "
        "line content as evidence, re-read the current generated TB and "
        "simulation_result.json directly in this workspace -- do not assume "
        "a previous attempt's description of the TB is still accurate."
    )


def _build_diagnosis_prompt(diagnosis_workspace: Path, tb_attempt: int) -> str:
    """Use the sanitized TB for deterministic scanning and its container path for the LLM."""
    host_tb = diagnosis_workspace / "tb"
    container_tb = Path(_container_workspace_path(host_tb)).as_posix()
    host_result = diagnosis_workspace / "simulation_result.json"
    container_result = Path(_container_workspace_path(host_result)).as_posix()
    plan_host = (
        HOST_WORKSPACE / DESIGN_GENERATED_ROOT / "results"
        / f"tb_update_plan_iteration_{tb_attempt}.yaml"
    )
    container_plan = Path(_container_workspace_path(plan_host)).as_posix()

    prompt = build_diagnosis_prompt(
        container_result,
        "specification.md",
        "reference_model.py",
        "verification_plan.yaml",
        tb_dir=str(host_tb),
        update_plan_path=container_plan,
    )
    prompt = prompt.replace(str(host_tb), container_tb)

    prior_context = _format_prior_tb_attempts_context(tb_attempt)
    prior_section = f"\n\n{prior_context}" if prior_context else ""

    prompt += f"""
{prior_section}
DIAGNOSIS REQUIREMENTS:
- The generated TB under {container_tb} is the ONLY TB source you may inspect.
- The RTL is unavailable and must remain unavailable. Never inspect, reconstruct,
  infer, or propose changes to the DUT RTL.
- Never infer DUT correctness from the observed implementation.
- Inspect the actual traceback and the generated source file/function/line named
  by the failure. Reproduce or trace the failure in the generated TB when practical.
- A Python exception, non-zero process exit, uncaught runtime error, or clean_exit=false
  is a failed validation execution even when uvm_errors and uvm_fatals are zero.
- If the same exception occurs in multiple unrelated tests and the traceback points
  into shared generated infrastructure, strongly prefer a generated-TB diagnosis.
- Do NOT classify a test as a DUT functional failure when the TB crashed before
  scoreboard comparison, assertions, or other meaningful checking could complete.

VERILATOR COMPILATION FAILURES:
- A Verilator compilation failure is NOT automatically a template_bug.
- First determine, using only the available diagnostic evidence, whether the failure
  is attributable to:
    1. the generated TB,
    2. shared verification/simulation infrastructure or templates, or
    3. the DUT/RTL.
- If the failure evidence explicitly points to an RTL/DUT source file or indicates
  that the DUT failed to compile before meaningful TB execution, DO NOT classify it
  as generation_bug or template_bug.
- An RTL/DUT compilation failure during TB validation must be classified as
  non_actionable because the TB diagnosis stage is RTL-blind and cannot safely
  establish a TB repair.
- For an RTL/DUT compilation failure, changes MUST be [].
- NEVER propose, describe, or request a modification to an RTL/DUT file as part of
  a TB repair plan.
- If the compilation failure points to a generated TB file and the failure mechanism
  is concretely identifiable, classify it as generation_bug and provide the exact
  generated TB file and minimal repair instructions.
- If the compilation failure points to shared verification infrastructure, simulator
  configuration, or a common verification template, classify it as template_bug.
- A template_bug means the shared infrastructure is defective; it MUST NOT contain
  per-generation repair changes.
- If the available evidence cannot reliably distinguish an RTL/DUT failure from a
  TB or infrastructure failure, classify it as non_actionable and set changes: [].
- Never speculate about the contents or correctness of an unavailable RTL file.

DECISION RULES:
- Distinguish generation_bug, template_bug, and non_actionable using concrete evidence.
- generation_bug:
    The failure is specific to this generated verification environment and can be
    repaired by modifying generated TB files.
- template_bug:
    The failure is caused by shared verification infrastructure, simulation
    configuration, or a common TB/template defect rather than this generated TB.
- non_actionable:
    The failure cannot be safely attributed to a repairable generated-TB defect
    using the available evidence. This includes RTL/DUT compilation failures and
    ambiguous failures where the TB-specific cause cannot be established.

CHANGE SAFETY:
- For generation_bug, root_cause.summary and root_cause.evidence MUST identify the
  concrete failure mechanism, generated file, and affected execution/checking state.
- Include the exact generated-TB file(s) to repair and minimal instructions.
- For template_bug, changes MUST be [].
- For non_actionable, changes MUST be [].
- Every proposed change for generation_bug MUST target a file inside the generated
  TB under {container_tb}.
- NEVER propose a change to the RTL/DUT, including .v, .sv, .vh, or .svh source files.
- Never use a generated-TB repair to compensate for, hide, bypass, or work around an
  RTL/DUT compilation failure.
- Never recommend deleting/weakening assertions, removing scoreboard/reference-model
  checks, disabling coverage, skipping tests, changing expected values, suppressing
  exceptions, or changing stimulus solely to avoid the observed failure.
- Never modify RTL, specification, reference model, or verification plan.

EVIDENCE REQUIREMENT:
- Do not infer the root cause merely from the fact that Verilator failed.
- Base the verdict on the concrete traceback, compiler diagnostic, generated-TB source,
  and execution state available to you.
- If the evidence identifies only an RTL/DUT compilation failure, report that the TB
  could not be meaningfully validated and classify it as non_actionable.
- If no generated-TB-specific repair can be justified from the evidence, do not invent one.

OUTPUT CONSISTENCY:
- verdict: template_bug MUST have changes: [].
- verdict: non_actionable MUST have changes: [].
- verdict: generation_bug may contain changes, but every changed file MUST be a
  generated-TB file.
- The verdict and proposed changes MUST be mutually consistent.
- Do not output an RTL file in changes under any circumstances.
"""
    return prompt


_SCENARIO_LIST_KEYS = ("directed_test_scenarios", "corner_cases")
_SCENARIO_CONTENT_KEYS = (
    "description", "stimulus", "expected", "note", "priority", "id",
)


def _drop_empty_scenario_stubs(parsed: dict) -> tuple[dict, list[str]]:
    """Remove scenario-list entries with no content beyond an empty/null name.

    Returns the (possibly unchanged) plan and a list naming what was dropped,
    e.g. "directed_test_scenarios[2]". An entry counts as a content-free stub
    only when every other schema field is missing or empty -- a scenario with
    a null/missing name but real stimulus/expected content is left alone and
    still fails validation with its original, more informative error.
    """
    dropped: list[str] = []
    pruned = dict(parsed)
    for key in _SCENARIO_LIST_KEYS:
        items = pruned.get(key)
        if not isinstance(items, list):
            continue
        kept = []
        for index, item in enumerate(items):
            has_name = isinstance(item, dict) and bool(str(item.get("name") or "").strip())
            has_other_content = isinstance(item, dict) and any(
                item.get(field) not in (None, "", [], {})
                for field in _SCENARIO_CONTENT_KEYS
            )
            if not has_name and not has_other_content:
                dropped.append(f"{key}[{index}]")
                continue
            kept.append(item)
        pruned[key] = kept
    return pruned, dropped


def ensure_directed_scenario_ids(plan_path: str | Path) -> str:
    """Give every directed scenario a stable ID (derived from its name if missing)."""
    # TestScenario.id is optional in the schema, but the manifest gate requires
    # IDs; an LLM plan with `id: null` otherwise crash-loops Stage 4 integration.
    plan_path = Path(plan_path)
    plan = yaml.safe_load(plan_path.read_text()) or {}
    scenarios = plan.get("directed_test_scenarios") or []
    seen = {
        str(s["id"]).strip() for s in scenarios
        if isinstance(s, dict) and str(s.get("id") or "").strip()
    }
    filled = []
    for s in scenarios:
        if not isinstance(s, dict) or str(s.get("id") or "").strip():
            continue
        base = re.sub(r"[^A-Za-z0-9_]+", "_", str(s.get("name", ""))).strip("_") or "scenario"
        candidate, n = base, 2
        while candidate in seen:
            candidate, n = f"{base}_{n}", n + 1
        s["id"] = candidate
        seen.add(candidate)
        filled.append(candidate)
    if filled:
        plan_path.write_text(yaml.safe_dump(plan, sort_keys=False, allow_unicode=True))
        print(f"⚠ Plan directed scenarios had no IDs; derived from names: {filled}")
    return plan_path.read_text()


def validate_scenario_manifest(plan_path: str | Path, tb_dir: str | Path) -> tuple[bool, list[str]]:
    """
    Validate the stable-ID mapping between the accepted verification plan
    and the generated cocotb + pyuvm directed environment.

    This is a deterministic gate. It does not use scenario-name matching
    and does not depend on a particular design name or generated filename.
    """
    plan_path = Path(plan_path)
    tb_dir = Path(tb_dir)

    errors: list[str] = []
    manifest_path = tb_dir / "generation_manifest.yaml"

    if not plan_path.is_file():
        return False, [f"Verification plan not found: {plan_path}"]
    if not manifest_path.is_file():
        return False, [f"Generation manifest not found: {manifest_path}"]

    try:
        plan = yaml.safe_load(plan_path.read_text()) or {}
        manifest = yaml.safe_load(manifest_path.read_text()) or {}
    except Exception as exc:
        return False, [f"Failed to parse plan/manifest: {exc}"]

    planned = plan.get("directed_test_scenarios") or []
    mapped = manifest.get("scenarios") or []

    if not isinstance(planned, list):
        return False, ["directed_test_scenarios must be a list"]
    if not isinstance(mapped, list):
        return False, ["manifest.scenarios must be a list"]

    planned_ids = [str(x.get("id", "")).strip() for x in planned if isinstance(x, dict)]
    mapped_ids = [str(x.get("id", "")).strip() for x in mapped if isinstance(x, dict)]

    if len(planned_ids) != len(planned):
        errors.append("Every planned directed scenario must be a mapping.")
    if len(mapped_ids) != len(mapped):
        errors.append("Every manifest scenario entry must be a mapping.")

    for index, scenario_id in enumerate(planned_ids):
        if not scenario_id:
            errors.append(f"Plan scenario at index {index} has no stable ID.")

    for scenario_id in sorted(set(x for x in mapped_ids if x)):
        if mapped_ids.count(scenario_id) != 1:
            errors.append(
                f"Scenario ID {scenario_id} appears {mapped_ids.count(scenario_id)} "
                "times in the manifest; it must appear exactly once."
            )

    planned_set = set(x for x in planned_ids if x)
    mapped_set = set(x for x in mapped_ids if x)

    missing = sorted(planned_set - mapped_set)
    extra = sorted(mapped_set - planned_set)
    if missing:
        errors.append(f"Manifest is missing planned scenario IDs: {missing}")
    if extra:
        errors.append(f"Manifest contains undeclared scenario IDs: {extra}")

    # Parse every generated Python source once. This makes the check
    # independent of the generated filename convention.
    python_files = [
        p for p in tb_dir.rglob("*.py")
        if "__pycache__" not in p.parts
    ]

    class_nodes: dict[str, tuple[Path, ast.ClassDef]] = {}
    aliases: dict[str, str] = {}

    for path in python_files:
        try:
            tree = ast.parse(path.read_text(), filename=str(path))
        except (SyntaxError, UnicodeDecodeError) as exc:
            errors.append(f"Cannot parse generated Python file {path}: {exc}")
            continue

        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                class_nodes.setdefault(node.name, (path, node))
            elif isinstance(node, (ast.Assign, ast.AnnAssign)):
                targets = []
                if isinstance(node, ast.Assign):
                    targets = [t for t in node.targets if isinstance(t, ast.Name)]
                    value = node.value
                else:
                    targets = [node.target] if isinstance(node.target, ast.Name) else []
                    value = node.value

                if isinstance(value, ast.Name):
                    for target in targets:
                        aliases[target.id] = value.id

    def resolve_symbol(name: str) -> str:
        """Resolve simple Python aliases without executing generated code."""
        seen = set()
        current = name
        while current in aliases and current not in seen:
            seen.add(current)
            current = aliases[current]
        return current

    def scenario_id_from_class(node: ast.ClassDef):
        for item in node.body:
            if isinstance(item, ast.Assign):
                for target in item.targets:
                    if isinstance(target, ast.Name) and target.id == "SCENARIO_ID":
                        if isinstance(item.value, ast.Constant) and isinstance(item.value.value, str):
                            return item.value.value
            elif isinstance(item, ast.AnnAssign):
                if (
                    isinstance(item.target, ast.Name)
                    and item.target.id == "SCENARIO_ID"
                    and isinstance(item.value, ast.Constant)
                    and isinstance(item.value.value, str)
                ):
                    return item.value.value
        return None

    def class_references_symbol(node: ast.ClassDef, symbol: str) -> bool:
        target = resolve_symbol(symbol)
        return any(
            isinstance(n, ast.Name) and resolve_symbol(n.id) == target
            for n in ast.walk(node)
        )

    for entry in mapped:
        if not isinstance(entry, dict):
            continue

        scenario_id = str(entry.get("id", "")).strip()
        sequence_name = str(entry.get("sequence", "")).strip()
        test_name = str(entry.get("test", "")).strip()

        # The manifest is a generated representation of the accepted plan.
        # Keep the human-readable scenario name synchronized too; the stable
        # ID remains the authoritative identity.
        planned_entry = next(
            (item for item in planned
             if isinstance(item, dict)
             and str(item.get("id", "")).strip() == scenario_id),
            None,
        )
        if planned_entry is not None:
            planned_name = str(planned_entry.get("name", "")).strip()
            manifest_name = str(entry.get("name", "")).strip()
            if manifest_name != planned_name:
                # Cosmetic only -- id already matched above, which is what makes
                # planned_entry authoritative. A generation stage legitimately
                # writing a nicer human-readable name than the plan's own name
                # field (e.g. a plan using slug-style names like "all_zero")
                # must not fail the hard gate below: with all sub-stage
                # checkpoints already marked done, "forcing regeneration" is a
                # no-op, so treating this as fatal crash-loops every restart
                # on the exact same mismatch (observed live on aes_benchmark,
                # 2026-09-17). Warn instead of erroring.
                print(
                    f"  (note) {scenario_id}: manifest scenario name "
                    f"{manifest_name!r} differs from plan name "
                    f"{planned_name!r}; id match is authoritative, continuing."
                )

        if not scenario_id:
            continue

        if not sequence_name:
            errors.append(f"{scenario_id}: missing sequence name.")
            continue
        if not test_name:
            errors.append(f"{scenario_id}: missing test name.")
            continue

        resolved_sequence = resolve_symbol(sequence_name)
        sequence_record = class_nodes.get(resolved_sequence)

        # A manifest sequence may be an alias of the actual class
        # (e.g. FullOverflowSequence -> MaximumOverflowSequence).
        if sequence_record is None:
            if sequence_name not in aliases:
                errors.append(
                    f"{scenario_id}: sequence symbol not found: {sequence_name}"
                )
                continue
            errors.append(
                f"{scenario_id}: sequence alias {sequence_name} resolves to "
                f"{resolved_sequence}, but that target class was not found."
            )
            continue

        sequence_path, sequence_node = sequence_record
        declared_id = scenario_id_from_class(sequence_node)

        # A generated sequence may inherit its SCENARIO_ID from a base
        # sequence. Resolve that inheritance deterministically without
        # importing or executing generated code.
        if declared_id is None:
            base_names = []
            for base in sequence_node.bases:
                if isinstance(base, ast.Name):
                    base_names.append(resolve_symbol(base.id))

            visited = set()
            pending = list(base_names)

            while pending:
                base_name = pending.pop(0)
                if base_name in visited:
                    continue
                visited.add(base_name)

                base_record = class_nodes.get(base_name)
                if base_record is None:
                    continue

                _, base_node = base_record
                inherited_id = scenario_id_from_class(base_node)

                if inherited_id is not None:
                    declared_id = inherited_id
                    break

                for base in base_node.bases:
                    if isinstance(base, ast.Name):
                        pending.append(resolve_symbol(base.id))

        if declared_id != scenario_id:
            errors.append(
                f"{scenario_id}: sequence {sequence_name} (resolved to "
                f"{resolved_sequence}) declares SCENARIO_ID={declared_id!r}, "
                f"not {scenario_id!r}."
            )

        test_record = class_nodes.get(resolve_symbol(test_name))
        if test_record is None:
            errors.append(f"{scenario_id}: test class not found: {test_name}")
            continue

        test_path, test_node = test_record

        # Prefer the explicit get_sequence_class() contract. If a generated
        # test intentionally orchestrates multiple sequences, accept a direct
        # reference to the mapped sequence anywhere in that test class.
        explicit_return = False
        for node in ast.walk(test_node):
            if isinstance(node, ast.FunctionDef) and node.name == "get_sequence_class":
                for child in ast.walk(node):
                    if isinstance(child, ast.Return) and isinstance(child.value, ast.Name):
                        if resolve_symbol(child.value.id) == resolved_sequence:
                            explicit_return = True

        if not explicit_return and not class_references_symbol(test_node, sequence_name):
            errors.append(
                f"{scenario_id}: test {test_name} does not reference "
                f"mapped sequence {sequence_name}."
            )

    return not errors, errors



def has_existing_generated_tb():
    """
    Return True when a previously generated Stage-4 cocotb+pyuvm
    environment is present and looks complete enough to continue from
    Stage 5.

    This intentionally checks for concrete Stage-4 artifacts rather than
    merely checking whether the generated TB directory exists. That prevents an
    interrupted/partial Stage-4 generation from being mistaken for a
    complete environment.

    The check is deliberately independent of Stage-4 checkpoint files:
    an existing TB is a valid starting point even when checkpoint markers
    were lost, copied, or never created.
    """
    tb_dir = TB_DIR
    if not tb_dir.is_dir():
        return False

    required = [
        tb_dir / "CONTRACT.md",
        tb_dir / "generation_manifest.yaml",
        tb_dir / "test_top.py",
    ]

    if not all(path.is_file() for path in required):
        return False

    # A generated environment must contain at least one Python source
    # beyond the top-level cocotb entry point.
    has_python_sources = any(
        path.suffix == ".py" and path.name != "test_top.py"
        for path in tb_dir.iterdir()
        if path.is_file()
    )
    if not has_python_sources:
        return False

    # The scenario manifest is part of the persistent Stage-4 state.
    # Reject an otherwise "complete-looking" TB if its stable scenario-ID
    # mapping does not agree with the accepted verification plan.
    valid, errors = validate_scenario_manifest(PLAN, tb_dir)
    if not valid:
        print("\n⚠ Existing generated TB has an invalid scenario manifest:")
        for error in errors:
            print(f"  - {error}")
        return False

    return True



# =========================================================

# Helper: create OpenCode + Bash

# =========================================================

def _container_workspace_path(path):
    """Map a host workspace path to its /workspace container path."""
    path = Path(path).resolve()
    host_root = HOST_WORKSPACE.resolve()

    try:
        relative = path.relative_to(host_root)
    except ValueError:
        # Preserve paths that are already container-visible or otherwise
        # outside this project's host workspace.
        return str(path)

    return str(Path(CONTAINER_WORKSPACE) / relative)


def create_improvement_agent(work_dir, retries=1):
    """Create an LLM agent whose filesystem contains only sanitized inputs.

    ``retries`` is configurable per caller so the normal diagnosis and
    verification-improvement agents retain their existing behavior, while
    the TB repair operation can tolerate a transient OpenCode timeout.
    """
    # Files are prepared/collected using the host path, but OpenCode and its
    # Bash tool execute inside ARGUS where the project is mounted at /workspace.
    container_work_dir = _container_workspace_path(work_dir)

    bash = BashTool(
        "verification_improvement_workspace",
        work_dir=container_work_dir,
        timeout_seconds=BASH_AGENT_TIMEOUT_SECONDS,
        task_options={"resources": {"opencode_tools": 1}},
    )
    llm = OpenCodeLLM(
        model=_load_llm_model(),
        work_dir=container_work_dir,
        timeout_seconds=LLM_TIMEOUT_SECONDS,
        retries=retries,
    )
    return llm, bash



def create_analysis_agent(work_dir):
    """Create the RTL-blind LLM agent used to analyze simulation evidence."""
    container_work_dir = _container_workspace_path(work_dir)

    bash = BashTool(
        "verification_analysis_workspace",
        work_dir=container_work_dir,
        timeout_seconds=BASH_AGENT_TIMEOUT_SECONDS,
        task_options={"resources": {"opencode_tools": 1}},
    )
    llm = OpenCodeLLM(
        model=_load_llm_model(),
        work_dir=container_work_dir,
        timeout_seconds=LLM_TIMEOUT_SECONDS,
        retries=LLM_RETRIES,
    )
    return llm, bash


def validate_weakness_report(report):
    """Validate the compact RTL-blind weakness-report contract."""
    if not isinstance(report, dict):
        return False, "Weakness report must be a YAML mapping."

    if report.get("schema_version") != "2.0":
        return False, 'schema_version must be "2.0".'

    summary = report.get("summary")
    weaknesses = report.get("weaknesses")
    constraints = report.get("constraints")
    authoritative_inputs = report.get("authoritative_inputs")

    if not isinstance(summary, dict):
        return False, "summary must be a mapping."
    for field in ("overall_status", "tests_total", "tests_passed", "tests_failed"):
        if field not in summary:
            return False, f"summary missing required field: {field}."

    if not isinstance(weaknesses, list):
        return False, "weaknesses must be a list."

    if not isinstance(constraints, dict):
        return False, "constraints must be a mapping."

    if constraints.get("rtl_exposed_to_llm") is not False:
        return False, "constraints.rtl_exposed_to_llm must be false."

    if not isinstance(authoritative_inputs, list):
        return False, "authoritative_inputs must be a list."

    required_inputs = {"specification", "reference_model", "verification_plan"}
    if not required_inputs.issubset(set(authoritative_inputs)):
        return False, "authoritative_inputs must include specification, reference_model, verification_plan."

    for index, weakness in enumerate(weaknesses):
        if not isinstance(weakness, dict):
            return False, f"weaknesses[{index}] must be a mapping."
        for field in (
            "id",
            "severity",
            "category",
            "title",
            "affected_tests",
            "evidence",
            "likely_cause",
            "investigation_targets",
            "recommended_action",
            "confidence",
        ):
            if field not in weakness:
                return False, f"weaknesses[{index}] missing required field: {field}."

    return True, report


def ensure_valid_weakness_report_yaml(llm, text, max_attempts=5):
    """
    Clean, parse, validate, and if necessary repair an RTL-blind weakness
    report.

    This is intentionally separate from ensure_valid_yaml() and
    ensure_valid_tb_update_plan_yaml(): a weakness report has its own
    schema and meaning. The repair call is restricted to YAML/schema
    conformance and must preserve the diagnosis meaning.
    """
    candidate = clean_yaml_response(text)
    last_error = None

    for attempt in range(max_attempts + 1):
        try:
            parsed = yaml.safe_load(candidate)
        except yaml.YAMLError as exc:
            parsed = None
            error = f"Invalid YAML: {exc}"
        else:
            valid, validation_result = validate_weakness_report(parsed)
            if valid:
                return yaml.safe_dump(
                    validation_result,
                    sort_keys=False,
                    allow_unicode=True,
                )
            error = validation_result

        last_error = error

        if attempt >= max_attempts:
            break

        print(
            f"\n⚠ Invalid verification weakness report YAML:\n{last_error}\n"
            f"Weakness-report YAML repair attempt "
            f"{attempt + 1}/{max_attempts}..."
        )

        prompt = f"""
You are a YAML syntax-and-schema repair tool for an RTL-blind verification
weakness report.

Repair the malformed document below so that it conforms to the exact
weakness-report contract.

IMPORTANT:
- Preserve the diagnosis meaning and all evidence-supported findings.
- Do NOT invent new weaknesses, causes, evidence, tests, or measurements.
- Do NOT expose, infer, or request RTL.
- Do NOT turn an inference into a fact.
- Do NOT remove a concrete weakness merely because the schema requires
  different field names.
- Convert the existing weakness information into the required schema
  fields without changing its meaning.
- If the source report contains `description`, preserve that content in
  the required `title`, `evidence`, `likely_cause`, or
  `recommended_action` fields as appropriate.
- `likely_cause` must remain clearly marked as an inference when the
  original report was uncertain.
- Preserve the reported test counts exactly.
- Preserve `constraints.rtl_exposed_to_llm: false`.
- Preserve the authoritative input list.
- Do not add `simulation_evidence`.
- Return ONLY the complete YAML document.
- Do NOT use Markdown fences.
- Do NOT add explanations before or after the YAML.

Required structure:

schema_version: "2.0"

summary:
  overall_status: pass | functional_failure | verification_weakness
  tests_total: <integer>
  tests_passed: <integer>
  tests_failed: <integer>

weaknesses:
  - id: <stable short identifier>
    severity: critical | high | medium | low
    category: <short category>
    title: <short title>
    affected_tests:
      - <test name>
    evidence:
      - <specific evidence from simulation_result.json or generated TB>
    likely_cause: <concise likely cause, clearly marked as inference>
    investigation_targets:
      - <specific generated-TB file/function to inspect>
    recommended_action: <specific verification-TB action>
    confidence: high | medium | low

constraints:
  rtl_exposed_to_llm: false

authoritative_inputs:
  - specification
  - reference_model
  - verification_plan

Parser/validation error:

---BEGIN ERROR---
{last_error}
---END ERROR---

Malformed weakness report:

---BEGIN DOCUMENT---
{candidate}
---END DOCUMENT---

Return ONLY the corrected weakness_report YAML.
"""

        response = llm_get(
            llm.prompt.chia_remote(
                llm,
                prompt,
                tools=[],
            )
        )

        if not response.success:
            raise RuntimeError(
                "OpenCode verification weakness-report YAML repair failed:\n"
                f"{response.stderr}"
            )

        repaired = response.result or ""
        if not repaired.strip():
            raise RuntimeError(
                "OpenCode verification weakness-report YAML repair "
                "returned an empty response."
            )

        candidate = clean_yaml_response(repaired)

    raise RuntimeError(
        "Unable to produce a valid weakness_report.yaml after "
        f"{max_attempts} repair attempts.\n\n"
        f"Last validation error:\n{last_error}"
    )


def build_analysis_prompt(workspace):
    """Prompt for the RTL-blind simulation-log analysis LLM."""
    return f"""
You are the verification-analysis LLM for a cocotb + pyuvm environment.

Work ONLY with the files available in your current workspace:
- specification.md
- reference_model.py
- verification_plan.yaml
- simulation_result.json
- the current generated TB under tb/

RTL IS NOT PROVIDED TO YOU.
rtl_info.json IS NOT PROVIDED TO YOU.
Do not search outside the current workspace for RTL or simulator source.
Do not request, infer, or attempt to access RTL files.

Your task is to analyze the raw simulation evidence and identify concrete
verification weaknesses that are useful to a later RTL-blind improvement
LLM.

IMPORTANT:
- simulation_result.json is the raw simulation evidence and is the
  authoritative source for what actually happened during this run.
- The deterministic metrics in the simulation result/report are useful
  objective measurements, but you must independently inspect the logs,
  test results, assertions, checker output, coverage output, and timing
  evidence available in simulation_result.json.
- A test passing does NOT automatically prove that the verification
  environment is strong.
- Look for false confidence, weak checking, stale/incorrect sampling,
  missing scenarios, ineffective assertions/checkers, coverage that does
  not exercise meaningful behavior, scoreboard blind spots, test isolation
  problems, reset/clocking weaknesses, and evidence that a test can pass
  without actually checking the intended behavior.
- Do not invent a defect merely because more coverage could be added.
- Only report weaknesses supported by concrete evidence in the supplied
  files.
- If the evidence is insufficient to establish a real weakness, do not
  report it.
- You are allowed to recommend changes to the generated verification
  environment, but you MUST NOT recommend changing the RTL.
- Keep the report compact and actionable.

Pay particular attention to temporal relationships between:
1. when a sequence/driver applies inputs,
2. when the monitor samples inputs and outputs,
3. when the scoreboard compares expected and observed values,
4. clock/reset boundaries,
5. and whether the observed transaction can actually be associated with
   the stimulus that produced it.

For a registered DUT, distinguish a genuine one-cycle observation
relationship from a monitor that merely samples stale values. Do not call
something a weakness solely because the monitor observes values from the
previous cycle if the scoreboard and transaction association are
demonstrably correct.

STRICT OUTPUT CONTRACT:
Return ONLY YAML. Do not use Markdown fences.

The YAML MUST have exactly this high-level structure:

schema_version: "2.0"

summary:
  overall_status: pass | functional_failure | verification_weakness
  tests_total: <integer>
  tests_passed: <integer>
  tests_failed: <integer>

weaknesses:
  - id: <stable short identifier>
    severity: critical | high | medium | low
    category: <short category>
    title: <short title>
    affected_tests:
      - <test name>
    evidence:
      - <specific evidence from simulation_result.json or TB>
    likely_cause: <concise likely cause, clearly marked as inference>
    investigation_targets:
      - <specific benchmark-local generated TB file/function to inspect>
    recommended_action: <specific verification-TB action>
    confidence: high | medium | low

constraints:
  rtl_exposed_to_llm: false

authoritative_inputs:
  - specification
  - reference_model
  - verification_plan

Do not include raw simulation logs in the report.
Do not include simulation_evidence.
Do not include RTL paths or RTL-derived facts unavailable in the workspace.
Do not include generic suggestions for additional coverage unless the
evidence demonstrates a concrete coverage weakness.

Write the report to weakness_report.yaml in the current workspace, and
also return the same YAML in your response.
"""

def create_agent():
    """Create the main generation agent in the selected benchmark workspace."""
    benchmark_workspace = _container_workspace_path(
        HOST_WORKSPACE / DESIGN_GENERATED_ROOT
    )

    bash = BashTool(
        "verification_workspace",
        work_dir=benchmark_workspace,
        timeout_seconds=BASH_AGENT_TIMEOUT_SECONDS,
        task_options={
            "resources": {"opencode_tools": 1}
        },
    )

    llm = OpenCodeLLM(
        model=_load_llm_model(),
        work_dir=benchmark_workspace,
        timeout_seconds=LLM_TIMEOUT_SECONDS,
        retries=LLM_RETRIES,
    )

    return llm, bash


# =========================================================

# Helper: clean LLM YAML output

# =========================================================

def clean_yaml_response(text):
    """Extract a YAML document from common LLM response formats."""

    if not text:
        return ""

    text = text.strip()

    # 1. Prefer fenced YAML blocks.
    if "```yaml" in text:
        start = text.find("```yaml") + len("```yaml")
        end = text.find("```", start)
        if end != -1:
            return text[start:end].strip()

    if "```" in text:
        start = text.find("```") + 3
        end = text.find("```", start)
        if end != -1:
            candidate = text[start:end].strip()
            if candidate.startswith("yaml"):
                candidate = candidate[4:].strip()
            return candidate

    # 2. Handle literal escaped prefixes.
    if text.startswith("yaml\\n"):
        text = text[5:].strip()

    if text.startswith("yaml\\r\\n"):
        text = text[7:].strip()

    # 3. If the LLM added prose before the YAML, locate the
    #    beginning of the YAML mapping.
    #    Anchor on line starts and include the version keys that precede
    #    `verdict:` in the contracts, or unfenced replies lose that line.
    match = re.search(
        r"^(?:schema_version|version|verdict|updates|changes|tb_update_plan):",
        text,
        re.M,
    )

    if match:
        text = text[match.start():].strip()

    return text



def parse_review_status(text):
    """
    Determine whether the reviewer found issues.

    The reviewer must emit NO_ISSUES or ISSUES_FOUND as a standalone status.
    The remainder of the response is free-form feedback and is passed to the
    repair LLM unchanged.
    """

    if not text or not text.strip():
        raise RuntimeError("Reviewer returned an empty response.")

    matches = []

    for line in text.splitlines():
        status = line.strip().upper()

        if status in {"NO_ISSUES", "ISSUES_FOUND"}:
            matches.append(status)

    if not matches:
        # Be tolerant of the model adding Markdown emphasis/headings.
        normalized = text.upper()
        if "ISSUES_FOUND" in normalized:
            return "issues_found"
        if "NO_ISSUES" in normalized:
            return "no_issues"

        raise RuntimeError(
            "Reviewer did not return NO_ISSUES or ISSUES_FOUND.\n\n"
            f"Raw reviewer response:\n{text}"
        )

    # If the model accidentally mentions both while reasoning, use the last
    # explicit status because it is most likely the final decision.
    return (
        "issues_found"
        if matches[-1] == "ISSUES_FOUND"
        else "no_issues"
    )



# Stage 2: Generate verification plan

# =========================================================

def generate_candidate_plan(llm, bash):

    canonical_schema = json.dumps(
        VerificationPlan.model_json_schema(),
        indent=2,
    )

    prompt = f"""

You are a senior digital-verification engineer.

Work in /workspace.

The plan you produce here is DUT/behavior-level and independent of the
target testbench implementation language. It will later be handed to a
downstream generator that implements the testbench in cocotb + pyuvm
(Python), not SystemVerilog UVM — so do not assume or require any
SystemVerilog-only verification features (e.g. do not depend on
covergroups, SVA, or classes existing in the plan itself; those are
concepts, and the downstream generator will realize them in Python).

CRITICAL OUTPUT CONTRACT:
Your YAML MUST conform exactly to the canonical VerificationPlan
Pydantic schema supplied below.

Do NOT invent an alternative YAML structure.
Do NOT rename fields.
Do NOT change mappings into lists or lists into mappings.
Do NOT use semantically similar field names when the schema specifies
a different name.

For example:
- reset MUST use "polarity" and "type" as defined by the schema.
- ports MUST be a mapping containing "inputs", "outputs", and "inouts".
- functional_behavior MUST be a mapping.
- each scenario's stimulus MUST match StimulusSequence.
- randomized_testing_strategy.constraints MUST contain RandomConstraint
  objects.
- functional_coverage MUST be a mapping containing covergroups.
- discrepancies MUST contain id, severity, and title.

## CANONICAL VerificationPlan JSON SCHEMA
{canonical_schema}

## MINIMAL WORKED EXAMPLE (structure only — invent nothing from this
## example itself; every field's VALUE must come from the real source
## files below)

dut:
  name: example_dut
  file: example_dut.sv
  description: One-line description of what the DUT does.
parameters:
  - name: WIDTH
    default: 8
    description: Data width in bits.
    test_values: [1, 8, 32]
clock_and_reset:
  clock:
    name: clk
    type: posedge
  reset:
    name: rst_n
    polarity: active-low
    type: asynchronous
    reset_value: {{}}
ports:
  inputs:
    - name: a
      width: 8
      description: Operand A.
  outputs:
    - name: y
      width: 8
      description: Result.
  inouts: []
functional_behavior:
  description: What the DUT does end to end.
  reset_behavior: What every output is immediately after reset.
directed_test_scenarios:
  - id: basic_add
    name: Basic addition
    priority: high
    stimulus:
      sequence:
        - action: reset
        - action: drive
          signals: {{a: 1}}
          cycles: 1
    expected: {{y: 1}}
corner_cases:
  - name: max_value_overflow
    description: Inputs at the maximum representable value.
    expected: Defined overflow behavior per spec.
randomized_testing_strategy:
  description: Randomize operand values across the legal range.
  constraints:
    - variable: a
      range: [0, 255]
  pass_criteria: Scoreboard reports zero mismatches.
functional_coverage:
  covergroups:
    - name: operand_cg
      bins:
        - name: a_bins
          variable: a
          bins:
            - name: zero
              range: [0, 0]
scoreboard_reference_model_strategy:
  description: Compare DUT output against the reference model every cycle
    a valid result is produced.
  pass_criteria: Exact match, zero tolerance.
useful_assertions:
  - name: y_stable_when_idle
    description: y must not change when no operation is active.
    severity: error
discrepancies: []

## COMMON MISTAKES THAT BREAK THE DOWNSTREAM GENERATOR — avoid every one
- `stimulus` is NEVER a bare list of `{{signal: value}}` dicts. It is
  always `{{sequence: [ {{action: ..., signals: {{...}}, cycles: N}}, ... ]}}`
  — a mapping with a `sequence` key, whose items are StimulusStep objects
  with an `action` field. A plain string step (e.g. `- reset`) is also
  accepted and becomes `{{action: reset}}` automatically, but only inside
  that `sequence` list, never as a bare top-level list.
- `ports` is a MAPPING with exactly the three keys `inputs`, `outputs`,
  `inouts` — never a flat list of ports with a `direction` field.
- `clock_and_reset.reset.polarity` must be exactly `active-low` or
  `active-high` (with the hyphen) — not `active_low`, `low`, or `0`.
- `clock_and_reset.reset.type` must be exactly one of `asynchronous`,
  `synchronous`, `asynchronous_assert_synchronous_deassert` — not
  `async`/`sync`.
- Every `directed_test_scenarios` entry needs a top-level `id` (a short,
  stable, lowercase_snake_case string). The downstream cocotb generator
  and the assertion/coverage stages reference scenarios BY this exact id
  — an entry with no `id`, or an `id` that changes between stages, breaks
  the generated environment.
- `functional_coverage.covergroups[].bins` are `CoverageVariable` objects
  (`{{name, variable, bins: [CoverageBin, ...]}}`), which is one level
  deeper than it looks: the group's own `bins` list holds variables, and
  each variable has its OWN nested `bins` list of `{{name, range}}` or
  `{{name, expression}}` value bins. Do not collapse these two levels.
- `randomized_testing_strategy.constraints[]` are `{{variable, range}}`
  objects, not raw expressions or strings.
- `discrepancies[]` entries need `id`, `severity`, and `title` at minimum
  — `severity` should be a short word (`low`/`medium`/`high`/`critical`),
  not a sentence.
- Do not wrap the whole document in a top-level key (e.g. `plan:` or
  `verification_plan:`). The fields above (`dut`, `parameters`, ... )
  are top-level.

Use the Bash tool to inspect ALL FOUR source files:

1. {RTL}

2. {RTL_INFO}

3. {SPEC}

4. {REF_MODEL}

You MUST inspect all four files before answering.

SOURCE AUTHORITY:

- RTL is the authoritative source for actual DUT structure only.

- rtl_info.json provides extracted structural information and may be
  used to confirm DUT structure and interface information.

- spec.md and ref_model.py are the authoritative sources for intended
  functional behavior and expected results.

- The RTL is the implementation under test. Its observed or implemented
  behavior MUST NOT be treated as the functional oracle.

- Never infer intended functional behavior, expected outputs, expected
  state transitions, protocol semantics, corner-case behavior, or
  scoreboard expectations from the RTL when those are defined by the
  specification and/or reference model.

- If the RTL implements behavior that differs from the specification
  or reference model, treat that difference as a potential DUT defect
  to be detected by the verification environment. Do NOT adapt the
  verification plan or expected behavior to match the incorrect RTL.

Generate a practical verification plan for a later cocotb+pyuvm

testbench generator.

The plan must contain:

- dut

- parameters

- clock_and_reset

- ports

- functional_behavior

- directed_test_scenarios

- corner_cases

- randomized_testing_strategy

- functional_coverage

- scoreboard_reference_model_strategy

- useful_assertions

- discrepancies

IMPORTANT DISTINCTION:

The plan is allowed to DESIGN verification activities.

Therefore, it is valid to introduce:

- directed stimulus values

- corner cases

- randomized testing strategies

- coverage points

- coverage crosses

- assertions

- scoreboard architecture

- test scenarios

These do NOT need to literally appear in the RTL/spec/reference

model because they are proposed verification strategies.

However, these proposed strategies MUST be consistent with

the source files.

Do NOT invent DUT facts.

Never invent:

- ports

- parameters

- signals

- clocks

- resets

- DUT functionality

- reference-model behavior

Use the RTL as the source of truth whenever describing the

implemented DUT structure and interface.

Use the specification and reference model as the source of truth

for intended functional behavior, expected results, functional

semantics, and verification expectations.

The RTL MUST remain the implementation under test and MUST NOT be

used to redefine, weaken, or adapt the functional verification

intent.

If RTL and specification or reference model disagree, explicitly

record the discrepancy instead of silently choosing the RTL behavior.

The discrepancy MUST NOT cause the plan to adopt the incorrect RTL

behavior as the expected behavior. The specification/reference-model

behavior remains the verification oracle unless the source files

themselves are genuinely ambiguous, in which case the ambiguity

must be explicitly recorded in discrepancies.

Preserve parameterization wherever applicable.

GOAL: produce a plan that is correct and usable as-is by a

downstream testbench generator. It does not need to be

exhaustive or maximally elaborate — a smaller, unambiguous,

fully-consistent plan is preferred over a larger plan that

reaches for extra coverage at the cost of precision.

## BEFORE YOU ANSWER — self-check every line
- Did you actually read all four source files with Bash, not guess from
  their filenames?
- Is `ports` a mapping with `inputs`/`outputs`/`inouts`, not a list?
- Does every `directed_test_scenarios` entry have an `id`?
- Is `stimulus` always `{{sequence: [...]}}`, never a bare list?
- Is `reset.polarity` exactly `active-low` or `active-high`?
- Did you invent any port, parameter, signal, clock, or reset that is
  not actually in the RTL? If yes, remove it.
- Did you silently adopt an incorrect RTL behavior as "expected" instead
  of recording a discrepancy? If yes, fix it.

Return ONLY valid YAML.

Do not use markdown fences.

Do not explain your answer.

Do not modify or create files.

"""

    response = llm_get(

        llm.prompt.chia_remote(

            llm,

            prompt,

            tools=[bash],

        )

    )

    if not response.success:

        if not (response.stderr or "").strip() and not (response.result or "").strip():

            # Chia's OpenCodeLLM.prompt() swallows subprocess.TimeoutExpired across
            # all retries and returns success=False with empty result/stderr instead
            # of raising -- no exception text for _record_llm_rate_limit to match on.
            # Mark it explicitly so a silent provider stall still triggers a cooldown
            # and model switch instead of looping on the same dead model forever.
            raise RuntimeError(
                "OpenCode generation failed: silent timeout, empty response "
                "(provider likely unavailable)"
            )

        raise RuntimeError(

            f"OpenCode generation failed:\n{response.stderr}"

        )

    result = response.result

    if not result or not result.strip():

        raise RuntimeError(

            "OpenCode returned an empty verification plan."

        )

    return result



# =========================================================

# Stage 3: Review verification plan

# =========================================================

def review_candidate_plan(llm, bash, candidate):

    """

    Independent semantic review.

    The reviewer checks source-grounded DUT facts and the

    semantic correctness of proposed verification strategies.

    It does NOT require every proposed test, coverage point,

    or assertion to literally exist in the source files.

    """

    prompt = f"""

You are an independent verification-plan auditor.

Work in /workspace.

A different LLM generated the candidate verification plan below.

Your job is to independently audit the candidate against

the source files.

Use Bash to inspect ALL FOUR:

1. {RTL}

2. {RTL_INFO}

3. {SPEC}

4. {REF_MODEL}

Candidate plan:

---BEGIN CANDIDATE---

{candidate}

---END CANDIDATE---

============================================================

WHAT THIS REVIEW IS FOR

============================================================

Your job is NOT to make the plan maximally exhaustive or

stylistically ideal. Your job is only to catch genuine,

verifiable defects that would cause a downstream testbench

generator to produce technically incorrect code (wrong

ports, wrong widths, wrong expected values, wrong reset

semantics, contradictions with the RTL, etc.).

A plan that is correct but could theoretically have MORE

coverage, MORE corner cases, or MORE elaborate assertions is

NOT a defect. Do not flag missing "nice to have" additions as

issues. If your only feedback is a suggestion for something

additional or a stylistic preference, that is not an issue —

respond NO_ISSUES.

============================================================

IMPORTANT REVIEW RULE

============================================================

Separate SOURCE FACTS from VERIFICATION DESIGN.

The candidate is ALLOWED to introduce new verification

activities that are not explicitly present in the sources.

For example, these are legitimate plan content:

- directed test scenarios

- chosen stimulus values

- corner cases

- randomized testing

- coverage bins

- coverage crosses

- scoreboard architecture

- assertions

- reset stress tests

- parameter sweep strategies

Do NOT mark those as hallucinations merely because they

were not written in the RTL or specification.

Instead, determine whether the proposed verification activity

is logically consistent with the DUT and reference behavior.

============================================================

CHECK SOURCE-GROUNDED DUT FACTS

============================================================

You MUST flag actual factual errors involving:

1. DUT/module identity

2. Parameters

3. Ports

4. Port directions

5. Port widths

6. Clock

7. Reset

8. Implemented DUT behavior

9. Reference-model behavior

10. RTL/specification discrepancies

============================================================

CHECK SEMANTIC CORRECTNESS

============================================================

You MUST also flag verification content when it is

semantically incorrect.

Examples:

- An expected arithmetic result is wrong.

- A reset test expects synchronous behavior from an

  asynchronous RTL reset.

- A scoreboard uses the wrong input/output cycle relationship.

- An assertion contradicts the actual RTL.

- A proposed coverage point is based on a nonexistent signal.

- A reference-model call is described incorrectly.

- A scenario claims overflow where the arithmetic cannot overflow.

- A test assumes combinational behavior from registered outputs.

============================================================

KNOWN RESET CHECK

============================================================

Pay special attention to asynchronous reset.

If the RTL contains:

    always @(posedge clk or negedge rst_n)

then rst_n assertion on negedge is asynchronous.

A plan saying reset only clears on posedge clk is incorrect.

If the specification says synchronous reset while the RTL

implements asynchronous reset, this MUST be reported as a

discrepancy.

Do not invent a required reset duration unless the sources

actually specify one.

============================================================

KNOWN REGISTERED-DATA CHECK

============================================================

Determine the actual relationship between input sampling and

registered outputs from the RTL.

Do not automatically call the DUT a "pipeline".

If the DUT simply samples inputs at posedge clk and registers

the result, describe that accurately.

============================================================

REFERENCE MODEL CHECK

============================================================

Compare arithmetic expectations against ref_model.py.

For every explicit numerical expected result in a directed

test, verify that it is mathematically correct.

============================================================

MATERIALITY BAR (READ CAREFULLY)

============================================================

Before reporting ISSUES_FOUND, ask yourself: "Would this

specific problem cause a testbench generated from this plan

to be functionally wrong, or to check against the wrong

expected behavior?"

If the honest answer is no — it's a matter of taste, added

thoroughness, phrasing, or a judgment call that is still

defensible given the sources — then it is NOT an issue.

When you are genuinely unsure whether something is a real

defect or just a stylistic nitpick, resolve the doubt in favor

of NO_ISSUES.

Only report ISSUES_FOUND for concrete, specific, checkable

problems you can point to directly in the candidate text.

============================================================

STRICT OUTPUT CONTRACT

Your response MUST begin with exactly one of these two lines:

NO_ISSUES

or:

ISSUES_FOUND

Do NOT put anything before the status line.

If there are no genuine, material, factual or semantic

problems, return:

NO_ISSUES

You may optionally provide a very short explanation after NO_ISSUES,
but do not provide unrelated analysis or suggestions for additions.

If there are one or more genuine, material problems, return:

ISSUES_FOUND

Then list ONLY the concrete problems that meet the materiality bar
above, and where useful, the correction needed to make the existing
candidate plan accurate.

Do NOT use YAML or JSON for the review response.

Do NOT rewrite the candidate plan.

Do NOT modify files.

Only report genuine, material factual or semantic problems.

Do NOT reject reasonable verification strategies merely because they
were newly designed by the generator.

Do NOT report an issue merely because the plan could be more thorough,
more exhaustive, or more elaborate — that is not a defect.

"""

    response = llm_get(

        llm.prompt.chia_remote(

            llm,

            prompt,

            tools=[bash],

        )

    )

    _raise_on_llm_failure(response, "review")

    result = response.result

    if not result or not result.strip():

        raise RuntimeError(

            "OpenCode reviewer returned an empty response."

        )

    return result.strip()



# Stage 4: Repair verification plan

# =========================================================

def format_prior_plan_reviews_context(history, current_attempt):
    """Summarize earlier same-run plan-review findings that did NOT get the
    candidate accepted, for inclusion in a later repair attempt's prompt.

    Without this, each repair only sees the single most-recent review, so a
    fix applied on attempt N can silently regress an issue an earlier
    review (attempt < N) already flagged and that this repair pass
    re-introduces -- the repair LLM has no way to know that happened.
    """
    prior = [
        entry for entry in history
        if isinstance(entry, dict)
        and entry.get("attempt") is not None
        and entry["attempt"] < current_attempt
    ]
    if not prior:
        return ""
    blocks = [
        f"--- Attempt {entry['attempt']} review ---\n{str(entry.get('review', '')).strip()}"
        for entry in prior
    ]
    return (
        "PRIOR REVIEW FINDINGS IN THIS SAME RUN (the candidate was rejected "
        "each time):\n\n" + "\n\n".join(blocks) + "\n\n"
        "These are provided so your edit does not resolve the latest finding "
        "by reintroducing a problem an earlier review already flagged. Check "
        "your fix against every prior finding above, not just the latest "
        "one, before returning the plan."
    )


def repair_candidate_plan(llm, bash, candidate, review, yaml_error=None, prior_reviews_context=""):
    """Repair the existing candidate using reviewer findings only."""

    prior_section = f"\n\n{prior_reviews_context}" if prior_reviews_context.strip() else ""

    prompt = f"""
You are a verification-plan editor.

An independent reviewer has already audited the candidate against the RTL,
rtl_info.json, specification, and reference model.

MINIMALLY EDIT THE EXISTING PLAN to fix the reported issues.

Do NOT regenerate the plan from scratch.
Do NOT redesign correct sections.
Preserve all correct content and the existing YAML structure.

---BEGIN CURRENT PLAN---
{candidate}
---END CURRENT PLAN---

---BEGIN REVIEW FINDINGS---
{review}
---END REVIEW FINDINGS---
{prior_section}

---BEGIN PREVIOUS YAML ERROR---
{yaml_error or "None"}
---END PREVIOUS YAML ERROR---

CANONICAL VerificationPlan SCHEMA:
{json.dumps(VerificationPlan.model_json_schema(), indent=2)}

The repaired document MUST conform exactly to this schema. Preserve the
existing plan's correct content, but when the current candidate uses a
structure that does not match the schema, convert it to the canonical
structure without changing its meaning.

Rules:
1. Go through the review findings one at a time. For each one, make a
   complete, unambiguous fix — not a partial edit or a hedge that only
   half-addresses the concern. A finding that is only partially fixed
   will very likely be flagged again on the next review pass, so resolve
   each one decisively using the sources you already have available in
   the current plan and findings text.
2. Make the smallest possible changes that still fully resolve every
   finding above.
3. Do not redesign or touch any section of the plan that the review
   findings did not mention.
4. Preserve correct tests, coverage, assertions, scoreboard details,
   parameters, ports, and DUT facts that were not flagged.
5. Do not invent DUT ports, parameters, clocks, resets, signals,
   functionality, or reference-model behavior.
6. Preserve genuine RTL/specification discrepancies.
7. If a YAML parser error is supplied, fix that syntax error.
8. Return the COMPLETE plan, not a patch.
9. Before finishing, re-read each review finding against your edited
   plan and confirm the exact concern raised no longer applies anywhere
   in the document.

ABSOLUTE OUTPUT REQUIREMENT:

Your response will be passed directly to yaml.safe_load().
Therefore the FIRST character of your response MUST be the first
character of the YAML document.

The response MUST begin with:

dut:

and MUST contain the complete verification plan.

Do not write:
- "Here is the corrected plan"
- "The fix is..."
- "I corrected..."
- explanations
- analysis
- Markdown
- code fences
- comments before the YAML
- Return ONLY the complete YAML document.
- Do NOT use Markdown fences.
- Do NOT explain the changes.
- Do NOT add text before or after YAML.
- Do NOT modify or create files.

If you need to reason about the fix, do that internally.
Your externally returned response must contain ONLY the YAML document.
"""

    response = llm_get(
        llm.prompt.chia_remote(
            llm,
            prompt,
            tools=[],
        )
    )

    if not response.success:
        raise RuntimeError(
            f"OpenCode repair failed:\n{response.stderr}"
        )

    result = response.result

    if not result or not result.strip():
        raise RuntimeError(
            "OpenCode repair returned an empty plan."
        )

    return result.strip()



def validate_tb_update_plan(plan):
    """
    Validate the diagnosis -> TB-repair YAML contract.

    This is intentionally separate from validate_yaml(), which validates the
    VerificationPlan schema.  A tb_update_plan is a different document with
    a much smaller, diagnosis-specific contract.
    """
    if not isinstance(plan, dict):
        return False, "TB update plan must be a YAML mapping."

    if str(plan.get("version", "")).strip() != "1.0":
        return False, 'version must be "1.0".'

    verdict = plan.get("verdict")
    if verdict not in {"generation_bug", "template_bug", "non_actionable"}:
        return False, (
            "verdict must be one of: generation_bug, template_bug, "
            "non_actionable."
        )

    root_cause = plan.get("root_cause")
    if not isinstance(root_cause, dict):
        return False, "root_cause must be a mapping."

    category = root_cause.get("category")
    valid_categories = {
        "compile",
        "elaboration",
        "connectivity",
        "driver",
        "monitor",
        "sequence",
        "scoreboard",
        "reference_model_integration",
        "reset",
        "timeout",
        "coverage",
        "assertion",
        "test",
        "simulation_control",
        "other",
    }
    if category not in valid_categories:
        return False, (
            "root_cause.category must be one of: "
            + ", ".join(sorted(valid_categories))
            + "."
        )

    for field in ("summary", "evidence"):
        if not isinstance(root_cause.get(field), str) or not root_cause[field].strip():
            return False, f"root_cause.{field} must be a non-empty string."

    changes = plan.get("changes")
    if not isinstance(changes, list):
        return False, "changes must be a list."

    valid_actions = {"modify", "add", "delete"}
    for index, change in enumerate(changes):
        if not isinstance(change, dict):
            return False, f"changes[{index}] must be a mapping."

        for field in ("file", "action", "reason", "instructions"):
            if field not in change:
                return False, f"changes[{index}] missing required field: {field}."

        if (
            not isinstance(change["file"], str)
            or not change["file"].strip()
        ):
            return False, f"changes[{index}].file must be a non-empty string."

        # The diagnosis contract says files are relative to the benchmark-local generated TB directory.
        # Keep the safety check deterministic and do not allow absolute paths
        # or traversal outside that tree.
        change_path = Path(change["file"])
        if change_path.is_absolute() or ".." in change_path.parts:
            return False, (
                f"changes[{index}].file must be a relative path under "
                "the benchmark-local generated TB."
            )

        if change["action"] not in valid_actions:
            return False, (
                f"changes[{index}].action must be one of: "
                "modify, add, delete."
            )

        if (
            not isinstance(change["reason"], str)
            or not change["reason"].strip()
        ):
            return False, f"changes[{index}].reason must be a non-empty string."

        if not isinstance(change["instructions"], list):
            return False, f"changes[{index}].instructions must be a list."

        for instruction_index, instruction in enumerate(change["instructions"]):
            if (
                not isinstance(instruction, str)
                or not instruction.strip()
            ):
                return False, (
                    f"changes[{index}].instructions[{instruction_index}] "
                    "must be a non-empty string."
                )

    validation = plan.get("validation")
    if not isinstance(validation, dict):
        return False, "validation must be a mapping."

    for field in ("tests", "requirements"):
        if not isinstance(validation.get(field), list):
            return False, f"validation.{field} must be a list."

        for index, item in enumerate(validation[field]):
            if not isinstance(item, str) or not item.strip():
                return False, (
                    f"validation.{field}[{index}] must be a non-empty string."
                )

    return True, plan


def ensure_valid_tb_update_plan_yaml(llm, text, max_attempts=5):
    """
    Clean, parse, validate, and if necessary repair a diagnosis YAML result.

    This deliberately mirrors the existing verification-plan YAML boundary
    without reusing validate_yaml(), because tb_update_plan.yaml has a
    different schema and meaning.
    """
    candidate = clean_yaml_response(text)
    last_error = None
    raw_text = text
    raw_dir = HOST_WORKSPACE / DESIGN_GENERATED_ROOT / "results" / "llm_raw"
    stamp = time.strftime("%Y%m%d_%H%M%S")

    for attempt in range(max_attempts + 1):
        try:
            parsed = yaml.safe_load(candidate)
        except yaml.YAMLError as exc:
            parsed = None
            error = f"Invalid YAML: {exc}"
        else:
            valid, validation_result = validate_tb_update_plan(parsed)
            if valid:
                return yaml.safe_dump(
                    validation_result,
                    sort_keys=False,
                    allow_unicode=True,
                )
            error = validation_result

        last_error = error
        raw_dir.mkdir(parents=True, exist_ok=True)
        (raw_dir / f"tb_update_plan_{stamp}_attempt{attempt}_rejected.txt").write_text(
            f"# model: {_SELECTED_LLM_MODEL}\n# error: {error}\n"
            f"# ---- raw response ----\n{raw_text}\n"
        )

        if attempt >= max_attempts:
            break

        print(
            f"\n⚠ Invalid TB update plan YAML:\n{last_error}\n"
            f"TB diagnosis YAML repair attempt "
            f"{attempt + 1}/{max_attempts}..."
        )

        prompt = f"""
You are a YAML syntax-and-schema repair tool for a TB diagnosis result.

Repair the malformed document below so that it conforms to the exact
tb_update_plan.yaml contract.

IMPORTANT:
- Preserve the diagnosis meaning and all correct values.
- Do not invent a different root cause.
- Do not change a valid verdict merely for formatting.
- Do not add unrelated TB changes.
- Do not change the specification, reference model, verification plan, or RTL.
- Keep the repair minimal.
- If prose surrounds the YAML, remove the prose.
- Return ONLY the complete YAML document.
- Do NOT use Markdown fences.
- Do NOT add explanations before or after the YAML.

Required structure:

version: "1.0"

verdict: generation_bug | template_bug | non_actionable

root_cause:
  category: compile | elaboration | connectivity | driver | monitor | sequence | scoreboard | reference_model_integration | reset | timeout | coverage | assertion | test | simulation_control | other
  summary: <short root cause>
  evidence: <specific evidence from the result>

changes:
  - file: <relative path under the benchmark-local generated TB directory>
    action: modify | add | delete
    reason: <why>
    instructions:
      - <minimal concrete change>

validation:
  tests:
    - <exact runnable test name>
  requirements:
    - build succeeds
    - no UVM_ERROR or UVM_FATAL
    - no reference-model mismatch

Parser/validation error:

---BEGIN ERROR---
{last_error}
---END ERROR---

Malformed diagnosis response:

---BEGIN DOCUMENT---
{candidate}
---END DOCUMENT---

Return ONLY the corrected tb_update_plan YAML.
"""

        response = llm_get(
            llm.prompt.chia_remote(
                llm,
                prompt,
                tools=[],
            )
        )

        if not response.success:
            raise RuntimeError(
                "OpenCode TB diagnosis YAML repair failed:\n"
                f"{response.stderr}"
            )

        repaired = response.result or ""
        if not repaired.strip():
            raise RuntimeError(
                "OpenCode TB diagnosis YAML repair returned an empty response."
            )

        raw_text = repaired
        candidate = clean_yaml_response(repaired)

    raise RuntimeError(
        "Unable to produce a valid tb_update_plan.yaml after "
        f"{max_attempts} repair attempts.\n\n"
        f"Last validation error:\n{last_error}"
    )


def validate_yaml(plan_text):

    """

    Python performs structural validation.

    Semantic correctness is handled by the LLM reviewer.

    """

    plan_text = clean_yaml_response(plan_text)

    try:

        parsed = yaml.safe_load(plan_text)

    except yaml.YAMLError as exc:

        return False, f"Invalid YAML: {exc}"

    if not isinstance(parsed, dict):

        return False, "Plan must be a YAML mapping."

    required_fields = [

        "dut",

        "parameters",

        "clock_and_reset",

        "ports",

        "functional_behavior",

        "directed_test_scenarios",

        "corner_cases",

        "randomized_testing_strategy",

        "functional_coverage",

        "scoreboard_reference_model_strategy",

        "useful_assertions",

        "discrepancies",

    ]

    for field in required_fields:

        if field not in parsed:

            return False, (

                f"Missing required top-level field: {field}."

            )

    return True, parsed



# =========================================================

# YAML boundary: clean + validate + repair

# =========================================================

def ensure_valid_yaml(llm, text, context, max_attempts=5):
    """
    Every LLM-produced YAML document passes through this boundary.

    Python first cleans and validates the response. If malformed, a small
    dedicated YAML-repair call is made. This helper fixes syntax/formatting
    only; semantic correctness remains the reviewer's responsibility.
    """

    candidate = clean_yaml_response(text)

    valid, error = validate_yaml(candidate)

    if valid:
        return candidate

    last_error = error

    for attempt in range(1, max_attempts + 1):

        print(f"\n⚠ Invalid YAML from {context}:")
        print(last_error)
        print(
            f"YAML syntax repair attempt "
            f"{attempt}/{max_attempts}..."
        )

        prompt = f"""
You are a YAML syntax repair tool.

Repair ONLY the YAML syntax and formatting of the document below.

Do NOT change its meaning.
Do NOT change values.
Do NOT add or remove verification-plan content.
Do NOT fix semantic verification errors.
Do NOT invent anything.

Parser error:

---BEGIN PARSER ERROR---
{last_error}
---END PARSER ERROR---

Malformed YAML:

---BEGIN MALFORMED YAML---
{candidate}
---END MALFORMED YAML---

Return ONLY the complete corrected YAML document.
Do NOT use Markdown fences.
Do NOT add explanations.
Do NOT add text before or after the YAML.
"""

        response = llm_get(
            llm.prompt.chia_remote(
                llm,
                prompt,
                tools=[],
            )
        )

        if not response.success:
            raise RuntimeError(
                f"OpenCode YAML repair failed for {context}:\n"
                f"{response.stderr}"
            )

        repaired = response.result

        if not repaired or not repaired.strip():
            raise RuntimeError(
                f"OpenCode YAML repair returned an empty response "
                f"for {context}."
            )

        candidate = clean_yaml_response(repaired)

        valid, error = validate_yaml(candidate)

        if valid:
            print(f"✓ YAML repaired successfully for {context}.")
            return candidate

        last_error = error

    raise RuntimeError(
        f"Unable to produce valid YAML for {context} after "
        f"{max_attempts} syntax-repair attempts.\n\n"
        f"Last YAML error:\n{last_error}"
    )



# =========================================================
# Shared, compact constraint block used by every cocotb+pyuvm
# generation stage below (kept short and separate so it isn't
# re-deriving conventions from a giant prompt each time).
#
# This MUST be a function, not a module-level f-string constant: it
# embeds {TB_DIR_REL}, and a module-level f-string is evaluated once at
# import time -- before configure_design() (called from main(), much
# later) has overwritten TB_DIR_REL/DESIGN_NAME from the actual
# --design-config argument. A frozen constant here silently told every
# generation stage, for every design ever run, to write its files under
# the hardcoded default design's tb dir ("adder", the module-level
# default at DESIGN_NAME's definition) instead of the real one --
# confirmed live: the coverage_assertions and integration stages for
# aes128_benchmark_corrupted wrote generated/designs/adder/tb/{coverage,
# assertions}.py instead of aes128_benchmark_corrupted's own tb dir.
# =========================================================

def _hard_constraints() -> str:
    return f"""
{pyuvm_api_reference()}
COCOTB + PYUVM COMPATIBILITY REQUIREMENTS (HARD, apply to every file you write):
- The testbench is implemented entirely in Python using cocotb + pyuvm.
  Do NOT generate any SystemVerilog UVM code: no SystemVerilog
  `interface`, no SV classes derived from uvm_driver/uvm_monitor/etc.,
  no SV covergroups, no SVA. The only SystemVerilog in this project is
  the DUT RTL itself, which already exists and must NOT be modified or
  duplicated.
- HARD VERSION CONSTRAINT: the generated verification environment MUST
  be compatible with **Cocotb 2.1.0**. Treat `cocotb==2.1.0` as the
  target runtime version for every generated Python file.
- Use only documented/public APIs available in Cocotb 2.1.0. Do NOT use
  private, internal, removed, deprecated, or version-specific APIs from
  other Cocotb releases.
- NEVER import `ModifiableObject` from `cocotb.handle`. Do not generate
  code that assumes `ModifiableObject` is present in `cocotb.handle`.
  When typing or accessing DUT handles, use public Cocotb 2.1.0 APIs and
  normal handle/value operations instead.
- Do not mix Cocotb APIs from different major versions. If an API is
  uncertain, prefer the documented Cocotb 2.1.0 API rather than guessing
  or copying an example written for another release.
- Before finishing a stage, review every `cocotb.*` import introduced by
  that stage and ensure that the imported symbol is part of the public
  Cocotb 2.1.0 API. A generated testbench with an unresolved Cocotb import
  is invalid even if its Python syntax is otherwise correct.
- Target cocotb's async/await coroutine style (`async def`,
  `cocotb.start_soon`, and triggers such as `RisingEdge`, `FallingEdge`,
  `ClockCycles`, `Timer`, `Combine` from `cocotb.triggers`) together with
  pyuvm as the UVM class library (`uvm_component`, `uvm_driver`,
  `uvm_monitor`, `uvm_agent`, `uvm_scoreboard`, `uvm_env`, `uvm_test`,
  `uvm_sequence`, `uvm_sequence_item`, `uvm_sequencer`,
  `uvm_analysis_port`/`uvm_tlm_analysis_fifo`, `ConfigDB`, etc.).
- There is no SystemVerilog `interface`. Drive and sample DUT pins
  directly through the `dut` handle cocotb provides (e.g.
  `dut.a.value = 1`, `dut.sum.value`), using exactly the pin names and
  widths recorded in CONTRACT.md.
- Share objects between UVM components (the `dut` handle, clock/reset
  signal names, any helper bus object) using pyuvm's
  `ConfigDB().set(...)` / `ConfigDB().get(...)`, keyed exactly as defined
  in CONTRACT.md. Do not invent alternative keys per component.
- Functional coverage must use the `cocotb-coverage` library
  (`cocotb_coverage.coverage.CoverPoint` / `CoverCross`, sampled through
  the shared `coverage_db`), not SystemVerilog covergroups.
- Assertions must be implemented as plain Python checks (an `assert`
  statement, or explicitly raising/flagging a failure) inside
  always-running checker coroutines started with `cocotb.start_soon`.
  These run concurrently with the rest of the environment and must fail
  the test immediately when a check does not hold. This replaces SVA.
- A watchdog is still required: an always-running coroutine (or a
  bounded `cocotb.triggers.with_timeout` wrapper around the test body)
  that fails the test if it does not finish within a bounded number of
  clock cycles, so a hung test cannot silently exhaust the whole
  simulation run.
- Do NOT invent DUT ports, parameters, clocks, resets, widths, or
  behavior. The RTL and rtl_info.json are authoritative for implemented
  DUT structure.
- Write all generated files under /workspace/{TB_DIR_REL}, as Python
  modules (.py) plus the build/run configuration described in the
  integration stage.
- The final generated environment must remain self-consistent with
  Cocotb 2.1.0: imports, coroutine/trigger usage, DUT-handle access,
  and simulator integration must all be compatible with that version.
- Do not leave a known-incompatible or legacy Cocotb import for a later
  stage to discover. If you encounter one, replace it with the documented
  Cocotb 2.1.0-compatible API without weakening verification.
- Do not modify the RTL, specification, reference model, or generated
  plan.

COMMON MISTAKES THAT BREAK SIMULATION (avoid every one):
- Reading a DUT signal with `dut.sig.value` gives a cocotb `LogicArray`
  (or `BinaryValue`), not a plain Python int. Comparing it directly to
  an int with `==` mostly works when the value is fully 0/1, but
  formatting it, using it as a dict key, or bit-slicing it does not
  behave like an int unless you convert first (`int(dut.sig.value)`).
  Always convert explicitly before arithmetic or comparisons that must
  be exact.
- A read of `dut.sig.value` immediately after a clock edge, before that
  edge has propagated, sees the PRE-edge value. Always `await
  RisingEdge(clk)` (or the correct edge per CONTRACT.md) before sampling
  a signal that is supposed to reflect that edge's effect.
- Every coroutine that drives or monitors the DUT must run under a
  bounded lifetime — either the test-level watchdog, or a loop condition
  tied to the test finishing. An unconditional `while True` coroutine
  with no exit path started via `cocotb.start_soon` can outlive the test
  and hang the NEXT test in the same run.
- ConfigDB keys are exact strings. A key of `"dut"` in one component and
  `"DUT"` or `"dut_handle"` in another silently fails to connect (raises
  at get() time, or worse, returns a stale default) — copy the exact key
  string from CONTRACT.md into every component that uses it, do not
  retype it from memory.
- `assert` statements inside a coroutine only fail the test if that
  coroutine's exception actually propagates to the test's await chain
  (e.g. via `cocotb.start_soon` + a saved handle you `await`, or a
  monitored `Combine`). A `cocotb.start_soon`'d coroutine whose
  exception is never awaited fails silently — the test can pass while a
  checker coroutine crashed. Reference model/scoreboard-driven checks
  should raise from code path that IS awaited by the test body.
"""

# Every later stage reads this instead of re-deriving pin/transaction
# conventions from scratch. It is written by Stage 4.1 (contract) and must
# be treated as authoritative by every subsequent stage.
CONTRACT_PATH = f"{TB_DIR_REL}/CONTRACT.md"

def consult_contract():
    return f"""
BEFORE WRITING ANYTHING:
Read /workspace/{CONTRACT_PATH}. It defines the DUT pin names, widths,
and directions (and how each is accessed via the `dut` handle), the
transaction/sequence-item field list, the exact ConfigDB key(s) used to
share the `dut` handle (and any helper object) between components, and
the top module name that were already fixed in an earlier generation
stage.

Do NOT redefine, rename, or reinterpret anything in the contract. If you
believe the contract is wrong or incomplete for what this stage needs,
extend it minimally (append, do not rewrite) and explain why in your
summary — do not silently diverge from it in code.
"""


def plan_section(canonical_plan, keys):
    """Return only the requested top-level keys of the plan, as YAML.

    Keeps each stage's prompt proportional to what it actually needs
    instead of always carrying the entire plan around.
    """
    parsed = yaml.safe_load(clean_yaml_response(canonical_plan))
    subset = {k: parsed[k] for k in keys if k in parsed}
    return yaml.safe_dump(subset, sort_keys=False, allow_unicode=True)


def run_stage(llm, bash, prompt, stage_name, tools=None):
    """Run one staged generation call and surface its summary."""
    response = llm_get(
        llm.prompt.chia_remote(
            llm,
            prompt,
            tools=[bash] if tools is None else tools,
        )
    )
    if not response.success:
        _raise_on_llm_failure(response, f"UVM generation stage '{stage_name}'")
    print(f"\n===== UVM GENERATION: {stage_name.upper()} =====")
    print((response.result or "").strip())
    return response


# =========================================================
# Stage 4.1 — Contract: transaction/sequence item + pin map
# =========================================================
def build_contract_prompt(canonical_plan, capabilities, rules, templates_note):
    plan_bits = plan_section(
        canonical_plan,
        ["dut", "parameters", "clock_and_reset", "ports", "functional_behavior"],
    )
    return f"""
You are the ARGUS cocotb+pyuvm environment generator, stage 1 of 6: CONTRACT.

Work in /workspace.

Establish the foundation every later stage will build on:
1. The transaction / sequence_item class, as a pyuvm `uvm_sequence_item`
   subclass with plain Python attributes for each field.
2. (Optional) a small Python helper module that centralizes direct DUT
   pin access via the `dut` handle, if that makes later stages cleaner.
   This is plain Python, NOT a SystemVerilog interface.
3. A short contract file at /workspace/{CONTRACT_PATH} that later stages
   will read instead of re-deriving conventions. It must state:
   - top module name
   - every DUT pin, with direction + width, and exactly how it is
     accessed (e.g. `dut.a`, `dut.sum`)
   - transaction/sequence_item field names + types
   - the exact ConfigDB key(s)/type(s) used to share the `dut` handle
     (and any helper object) between components
   - clock and reset signal names and reset polarity/type

{_hard_constraints()}

## RELEVANT VERIFICATION PLAN SECTIONS
{plan_bits}

## SOURCE FILES
RTL: {RTL}
RTL INFO: {RTL_INFO}
SPECIFICATION: {SPEC}

## UVM CAPABILITIES
{capabilities}

## UVM RULES
{rules}

{templates_note}

Use Bash to inspect the RTL and rtl_info.json before writing anything —
the pin map in CONTRACT.md must exactly match the DUT.

Do not generate the driver, monitor, sequencer, agent, scoreboard,
coverage, assertions, environment, tests, or top-level testbench in this
stage. Only the transaction/sequence_item class, the optional pin-access
helper, and CONTRACT.md.

Return a concise summary of what you wrote. Do not return source code in
your response — write it to disk.
"""


# =========================================================
# Stage 4.2 — Stimulus: sequencer, driver, sequences
# =========================================================
def build_stimulus_prompt(canonical_plan):
    plan_bits = plan_section(
        canonical_plan,
        ["directed_test_scenarios", "corner_cases", "randomized_testing_strategy"],
    )
    return f"""
You are the ARGUS cocotb+pyuvm environment generator, stage 2 of 6: STIMULUS.

Project files are mounted at /workspace. Use the benchmark-local generated
root /workspace/{DESIGN_GENERATED_ROOT} for all generated artifacts.

{consult_contract()}

{_hard_constraints()}

Generate the pyuvm sequencer (`uvm_sequencer`), driver (`uvm_driver`),
and sequences (`uvm_sequence` subclasses — directed, corner-case, and
randomized) that realize the following plan sections:

{plan_bits}

DIRECTED-SCENARIO ID CONTRACT (MANDATORY):
- Every entry under `directed_test_scenarios` has a stable `id`.
- Implement every planned directed scenario exactly once.
- Preserve each scenario's exact plan ID; IDs are authoritative and must
  never be renamed, normalized, inferred from Python class names, or
  replaced by scenario-name matching.
- Every generated directed sequence class MUST contain a class-level
  declaration:
      SCENARIO_ID = "<exact plan scenario id>"
- The value of `SCENARIO_ID` must exactly match the corresponding
  `directed_test_scenarios[].id`.
- Do not create an additional directed sequence representing a new
  scenario that is not present in the plan.
- Python class names do not have to match scenario names. The stable ID is
  the identity used to connect the plan to the generated testbench.
- Corner-case and randomized sequences are separate verification
  strategies. Do not assign them a directed scenario ID unless they
  directly implement one of the declared directed scenarios.

The driver must drive the DUT pins exactly as defined in CONTRACT.md,
synchronized to clock edges using cocotb triggers (e.g.
`await RisingEdge(dut.clk)`). Sequences must produce sequence_item
fields exactly as defined in CONTRACT.md — do not add, rename, or drop
fields.

Before finishing, inspect all generated sequence classes and verify that
every planned directed scenario ID appears exactly once as a
`SCENARIO_ID`.

Do not generate the transaction class, monitor, agent, scoreboard,
coverage, assertions, environment, tests, or top-level testbench in this
stage.

Return a concise summary of what you wrote. Do not return source code in
your response — write it to disk.
"""


# =========================================================
# Stage 4.3 — Observation: monitor, agent
# =========================================================
def build_observation_prompt(canonical_plan):
    plan_bits = plan_section(canonical_plan, ["functional_behavior"])
    return f"""
You are the ARGUS cocotb+pyuvm environment generator, stage 3 of 6: OBSERVATION.

Work in /workspace.

{consult_contract()}

{_hard_constraints()}

Generate the pyuvm monitor (`uvm_monitor`) and agent (`uvm_agent`,
encapsulating driver/sequencer from the previous stage and this stage's
monitor). The monitor must sample the DUT pins, using an
`async def` coroutine synchronized on the clock (e.g.
`await RisingEdge(dut.clk)`), using exactly the pins and timing
relationship described in CONTRACT.md and the functional behavior
below, and must publish transactions with exactly the fields defined in
CONTRACT.md via a `uvm_analysis_port`.

## RELEVANT VERIFICATION PLAN SECTIONS
{plan_bits}

Do not generate the transaction class, sequencer, driver, sequences,
scoreboard, coverage, assertions, environment, tests, or top-level
testbench in this stage.

Return a concise summary of what you wrote. Do not return source code in
your response — write it to disk.
"""


# =========================================================
# Stage 4.4 — Scoreboard + reference-model integration
# =========================================================
def build_scoreboard_prompt(canonical_plan):
    plan_bits = plan_section(
        canonical_plan, ["scoreboard_reference_model_strategy", "functional_behavior"]
    )
    return f"""
You are the ARGUS cocotb+pyuvm environment generator, stage 4 of 6: SCOREBOARD.

Work in /workspace.

{consult_contract()}

{_hard_constraints()}

Generate the scoreboard (`uvm_scoreboard`) and its reference-model
integration, following:

{plan_bits}

REFERENCE MODEL (behavioral oracle for expected results):
{REF_MODEL}

Use Bash to read ref_model.py before writing the scoreboard. The
reference model is already plain Python, so the scoreboard MUST import
and call it directly rather than reimplementing, regenerating, or
translating its logic. Either add the reference model's directory to
`sys.path` and `import` it, or copy ref_model.py verbatim into
/workspace/{TB_DIR_REL} and import it from there — do not hand-write a
second copy of its arithmetic/behavior. If you copy the file, note its
new path in your summary.

The scoreboard must consume monitor transactions (via a
`uvm_tlm_analysis_fifo` or an analysis export) with exactly the fields
defined in CONTRACT.md, and must apply the correct input/output cycle
relationship (combinational vs. registered) as established in CONTRACT.md
and functional_behavior above — do not assume a pipeline unless the DUT
actually is one.

Do not generate the transaction class, sequencer, driver, sequences,
monitor, agent, coverage, assertions, environment, tests, or top-level
testbench in this stage.

Return a concise summary of what you wrote. Do not return source code in
your response — write it to disk.
"""


# =========================================================
# Stage 4.5 — Coverage + assertions
# =========================================================
def build_coverage_assertions_prompt(canonical_plan):
    plan_bits = plan_section(canonical_plan, ["functional_coverage", "useful_assertions"])
    return f"""
You are the ARGUS cocotb+pyuvm environment generator, stage 5 of 6: COVERAGE & ASSERTIONS.

Work in /workspace.

{consult_contract()}

{_hard_constraints()}

Generate functional coverage and assertions per:

{plan_bits}

Functional coverage MUST be implemented with the `cocotb-coverage`
library (`cocotb_coverage.coverage.CoverPoint` / `CoverCross`, sampled
through the shared `coverage_db`), driven off transactions/fields
published by the monitor from stage 3. Do not invent coverage points on
pins or fields that do not exist in CONTRACT.md.

Assertions MUST be implemented as Python checker coroutines started with
`cocotb.start_soon` that run continuously (synchronized on clock edges
via cocotb triggers) and use plain `assert` statements (or an equivalent
explicit failure) to flag a violation immediately — this replaces SVA.
Each assertion coroutine should have a clear name/docstring tying it back
to the plan item it implements.

Do not generate the transaction class, sequencer, driver, sequences,
monitor, agent, scoreboard, environment, tests, or top-level testbench in
this stage.

Return a concise summary of what you wrote. Do not return source code in
your response — write it to disk.
"""


# =========================================================
# Stage 4.6 — Integration: env, tests, top-level cocotb entry point,
# build/run config, manifest
# =========================================================
def build_integration_prompt(canonical_plan, capabilities, rules, templates_note):
    return f"""
You are the ARGUS cocotb+pyuvm environment generator, stage 6 of 6: INTEGRATION.

Work in /workspace.

{consult_contract()}

{_hard_constraints()}

## CRITICAL DIRECTED TEST ↔ SEQUENCE CONTRACT

The deterministic `validate_scenario_manifest()` validator performs
static AST-based validation of the generated Python code.

For EVERY directed scenario in the verification plan, the generated
test class MUST explicitly reference its mapped sequence class.

For a manifest entry such as:

  id: reset_basic
  sequence: ResetBasicSequence
  test: TestResetBasic

the generated test class MUST have this form:

  class TestResetBasic(Sha256SingleSequenceTest):
      def get_sequence_class(self):
          return ResetBasicSequence

IMPORTANT:
- `ResetBasicSequence` MUST be an actual Python class symbol.
- Do NOT use only:
      SEQUENCE_NAME = "reset_basic"
- Do NOT use only:
      run_sequence("reset_basic")
- Do NOT use only:
      ALL_SEQUENCES["reset_basic"]
- Do NOT hide the sequence association entirely inside a base class.
- The test class itself must contain an explicit
  `get_sequence_class()` method whose return value is the mapped
  sequence class symbol.
- The return value must NOT be a string.
- The sequence class must still declare:
      SCENARIO_ID = "reset_basic"
- The manifest `sequence` field must contain the exact Python class
  name returned by `get_sequence_class()`.
- The manifest `test` field must contain the exact generated test
  class name.

Generate this explicit relationship for every directed scenario.

The reason this explicit method is required is that the deterministic
validator does not execute generated Python. It verifies the
test-to-sequence relationship statically through the Python AST.

Example:

  class TestKnownAnswerAbc(Sha256SingleSequenceTest):
      def get_sequence_class(self):
          return KnownAnswerAbcSequence

This explicit class-symbol reference is REQUIRED even if a registry
or SEQUENCE_NAME mechanism would be functionally equivalent.

Before finishing Stage 4 integration, inspect every directed test
class and verify that its `get_sequence_class()` returns the exact
sequence class named in the manifest.

Start by running `ls -la /workspace/{TB_DIR_REL}` (and inspect files as
needed) to see everything the previous five stages already wrote:
transaction class, sequencer/driver/sequences, monitor/agent,
scoreboard, coverage, assertions.

Now assemble the complete environment:
- environment (`uvm_env`) instantiating agent + scoreboard + coverage +
  the assertion checker coroutines from stage 5
- test classes (`uvm_test` subclasses) per the plan's
  directed/corner/randomized scenarios
- a top-level Python cocotb entry point at the EXACT, non-negotiable
  path `{TB_DIR_REL}/{TB_TEST_MODULE}.py` — the sim worker only ever
  loads a module literally named `{TB_TEST_MODULE}`, so any other
  filename (e.g. `tb_top.py`, `<design>_top.py`, or anything else that
  "sounds right") will build and pass static checks but then fail
  EVERY actual simulation run with "Generated Cocotb entry point
  missing". Do not invent or rename this module for any reason. It
  must contain a small number of `@cocotb.test()` coroutines that:
  - read which UVM test class to run from an environment variable
    (e.g. `os.environ.get("UVM_TESTNAME", ...)`), mirroring how SV UVM
    is normally driven by `+UVM_TESTNAME`
  - start the clock with `cocotb.clock.Clock` and apply reset per
    CONTRACT.md's clock/reset signal names and polarity
  - call `await uvm_root().run_test(<test_class_name>)`
  - are wrapped by the watchdog described in HARD_CONSTRAINTS
- build/run configuration for the cocotb flow MUST use a cocotb
  `Makefile` with `SIM = verilator`, `TOPLEVEL = <top module>`,
  `MODULE = {TB_TEST_MODULE}` (this exact value — not the design name,
  not any other descriptive name, not derived from the top module
  name), and `VERILOG_SOURCES` pointing only at the DUT RTL file(s).
  Do not use `cocotb.runner` or `get_runner`. There is no generated
  SystemVerilog to compile — only the existing DUT RTL is passed to
  the simulator as an HDL source.

## UVM CAPABILITIES
{capabilities}

## UVM RULES
{rules}

{templates_note}

MANDATORY SIMULATION MANIFEST:
Create/update /workspace/{TB_DIR_REL}/generation_manifest.yaml with:

PATHS IN THIS MANIFEST (read before writing it):
Every path below is resolved against /workspace, NOT against the directory
the manifest sits in. Two forms work:

    benchmarks/<design>/<dut>.sv              CORRECT (preferred)
    /workspace/benchmarks/<design>/<dut>.sv   CORRECT (also accepted)

    ../benchmarks/<design>/<dut>.sv           WRONG - "../" climbs out of
                                              /workspace, and the simulation
                                              is rejected before any test
                                              runs with the error
                                              "Manifest file escapes workspace"

A path must never contain "../". `top_file` and `compile_files` must use the
SAME form: if they name the same file, the two strings must be byte-identical.

top_module: <exact top module name, matching CONTRACT.md>
top_file: <path to the .sv file containing that module, in the form above>
compile_files:
  - <HDL source file(s) that must be passed directly to the simulator,
    each in the form above — this is only the existing DUT RTL; there
    must be no generated SystemVerilog listed here>
test_classes:
  - <exact pyuvm uvm_test class names, selectable via the UVM_TESTNAME
    environment variable read by the top-level cocotb entry point>
python_test_module:
  {TB_TEST_MODULE}

scenarios:
  - id: <exact directed_test_scenarios[].id from verification_plan.yaml>
    name: <exact scenario name from verification_plan.yaml>
    sequence: <exact generated directed sequence class implementing this ID>
    test: <exact pyuvm uvm_test class that runs this scenario>

The `scenarios` list is the authoritative mapping between the accepted
verification plan and the generated directed verification environment.

MANDATORY SCENARIO-MANIFEST RULES:
- Include every `directed_test_scenarios` entry exactly once.
- Preserve the exact scenario IDs from the verification plan.
- Do not invent additional directed scenario IDs.
- Sequence and test class names must be the actual generated Python
  class names.
- A Python test class may orchestrate more than one sequence where the
  plan requires it; the manifest still records the exact sequence/test
  responsible for each directed scenario.
- Do not use scenario-name normalization or substring matching as a
  substitute for IDs.
- The manifest must agree with the `SCENARIO_ID` declared by each
  directed sequence.
- The `scenarios` list is EXCLUSIVELY for `directed_test_scenarios`
  entries. `corner_cases` and `randomized_testing_strategy` are
  SEPARATE plan sections with their own IDs (e.g. `cc1`, `rand1`) --
  their generated tests/sequences must still exist and be runnable
  (referenced in `test_classes`), but their IDs must NEVER appear as
  `scenarios` entries. The deterministic validator rejects any
  `scenarios` entry whose `id` is not in `directed_test_scenarios` as
  an "undeclared scenario ID" and fails the whole stage -- this has
  been the single most common cause of this stage failing to complete.
  Before finishing, check every `scenarios[].id` against
  `directed_test_scenarios[].id` in verification_plan.yaml and delete
  any entry that is not an exact match (including any corner_case or
  randomized-test entry you may have added).

Do not list the reference model, driver, monitor, scoreboard, coverage,
or any other generated Python file in compile_files — those are not HDL
sources and must not be passed to the simulator.

FINAL COCOTB 2.1.0 COMPATIBILITY AUDIT (MANDATORY):
Before the final consistency pass, inspect ALL generated Python files
under /workspace/{TB_DIR_REL} and verify:

- Every `cocotb.*` import uses a public API available in Cocotb 2.1.0.
- There is NO import of `ModifiableObject` from `cocotb.handle`.
- There are no imports copied from examples targeting another Cocotb
  major version.
- All coroutine and trigger usage follows the Cocotb 2.1.0 async API.
- DUT handles are accessed through public Cocotb APIs; do not rely on
  private handle implementation details.
- If the environment provides Cocotb at runtime, run a lightweight
  import/version check (for example, `python -c "import cocotb; print(cocotb.__version__)"`)
  and confirm it reports 2.1.0. Do not change the installed environment
  from this prompt; only make the generated TB compatible with it.
- If any incompatibility is found, fix the generated TB and repeat the
  audit. Do not remove tests, assertions, scoreboards, coverage, or
  exception handling merely to make the import succeed.

FINAL CONSISTENCY PASS (do this after assembling everything):
Inspect ALL generated files together for:
- undeclared or misnamed DUT pins
- wrong DUT pin names or widths (cross-check against CONTRACT.md)
- missing UVM component construction or ConfigDB wiring
- incorrect reset handling
- incorrect scoreboard latency
- incorrect reference-model invocation (must call the real ref_model.py,
  not a reimplementation)
- missing files referenced by other generated files
- field-name mismatches between transaction, driver, monitor, scoreboard,
  and coverage (all must match CONTRACT.md exactly)
- any accidental SystemVerilog UVM code that should not exist
- scenario-ID consistency: every planned directed scenario ID appears
  exactly once in `generation_manifest.yaml`
- every manifest scenario points to an existing generated sequence class
  and test class
- every directed sequence's `SCENARIO_ID` exactly matches its manifest ID
- no undeclared directed scenario ID has been introduced
- the top-level cocotb entry point file is named EXACTLY
  `{TB_TEST_MODULE}.py` (run `ls /workspace/{TB_DIR_REL}` and confirm
  this exact filename exists) and the Makefile's `MODULE` line is
  exactly `MODULE = {TB_TEST_MODULE}` — a mismatch here passes this
  stage silently and only fails deep inside the next simulation run
Fix anything you find before finishing.

Do not return generated source code in your response. Return a concise
summary of the files created/modified and any remaining concerns.
"""


# =========================================================

# Stage 4 + 5: UVM environment generation and validation

# =========================================================
#
# Factored out of main() so it can be reached either after a freshly
# accepted verification plan, or directly when a verification plan and
# rtl_info.json already exist on disk (see main()).
#
# Stage 4 (generation) is broken into six focused calls that share a
# single CONTRACT.md instead of one giant prompt trying to hold every
# component + the full plan + full capabilities/rules at once. The
# testbench produced by these stages is cocotb + pyuvm (Python) driving
# the existing DUT RTL directly — no SystemVerilog UVM code is
# generated. Stage 5 (closed-loop simulate/diagnose/repair) is unchanged
# from the original SV-based flow structurally, but now operates over
# the generated Python cocotb+pyuvm environment.
#
# RESUMABILITY: each of the six sub-stages below, plus the closed-loop
# validation, is guarded by a checkpoint (see the Checkpoints section
# near the top of this file). If a checkpoint already exists from a
# previous run, that sub-stage is skipped entirely instead of being
# regenerated. This means re-running the pipeline after an interruption
# (crash, manual stop, quota limit, etc.) picks up exactly where it left
# off instead of redoing already-completed work.
#
def generate_and_validate_uvm(llm, bash, canonical_plan):

    # IMPORTANT: the benchmark-local TB directory is persistent verification state. If a
    # complete Stage-4 environment already exists, do NOT regenerate it
    # merely because the Stage-4 checkpoint markers are missing. This is
    # what allows `run8.py` to resume at Stage 5+ on a subsequent run.
   
    if has_existing_generated_tb() and not FORCE_SCENARIO_MANIFEST_REGEN:
        print(
            "\n✓ Existing generated cocotb+pyuvm environment found."
            "\n  Skipping Stage 4 generation and preserving the existing TB."
            "\n  Continuing directly with Stage 5+ validation/improvement."
        )
        run_verification_improvement_loop(llm, bash, canonical_plan)
        run_rtl_verification_loop()
        return

    print("\n[4/4] Generating cocotb+pyuvm environment (staged)...")

    # If a previously generated TB exists but its scenario manifest is
    # missing or inconsistent with the accepted plan, preserve all earlier
    # generated components and rerun only the integration stage that owns
    # the manifest. This avoids unnecessary LLM regeneration while
    # preventing an invalid (or entirely absent) manifest from being
    # carried into simulation.
    #
    # Deliberately not gated on the manifest file existing: if the
    # 'integration' LLM stage ran, got marked done, but never actually
    # wrote generation_manifest.yaml, every restart used to skip straight
    # past this guard (file doesn't exist -> condition False), then hit
    # the hard gate below with the same "not found" error every time --
    # an unrecoverable crash-loop. validate_scenario_manifest() already
    # treats a missing manifest as invalid, so it alone is sufficient.
    if TB_DIR.is_dir():
        manifest_valid, manifest_errors = validate_scenario_manifest(PLAN, TB_DIR)
        if not manifest_valid:
            print("\n⚠ Existing scenario manifest is invalid; "
                  "forcing UVM integration regeneration.")
            for error in manifest_errors:
                print(f"  - {error}")
            clear_stage("uvm_integration")

    capabilities = (HOST_WORKSPACE / "uvm_generator/capabilities.yaml").read_text()
    rules = (HOST_WORKSPACE / "uvm_generator/rules.yaml").read_text()
    templates_note = (
        "## TEMPLATE LIBRARY\n"
        "Inspect the available templates under /workspace/templates before "
        "writing files, and reuse them where applicable."
    )

    # Each entry: (checkpoint name, human label, prompt builder).
    # The prompt builder is only invoked if the stage isn't already done,
    # so we don't waste time re-reading the plan/capabilities/rules for
    # stages we're about to skip.
    stage_defs = [
        (
            "uvm_contract",
            "contract",
            lambda: build_contract_prompt(
                canonical_plan, capabilities, rules, templates_note
            ),
        ),
        (
            "uvm_stimulus",
            "stimulus",
            lambda: build_stimulus_prompt(canonical_plan),
        ),
        (
            "uvm_observation",
            "observation",
            lambda: build_observation_prompt(canonical_plan),
        ),
        (
            "uvm_scoreboard",
            "scoreboard",
            lambda: build_scoreboard_prompt(canonical_plan),
        ),
        (
            "uvm_coverage_assertions",
            "coverage_assertions",
            lambda: build_coverage_assertions_prompt(canonical_plan),
        ),
        (
            "uvm_integration",
            "integration",
            lambda: build_integration_prompt(
                canonical_plan, capabilities, rules, templates_note
            ),
        ),
    ]

    for checkpoint_name, stage_name, build_prompt in stage_defs:
        if is_stage_done(checkpoint_name):
            print(
                f"\n✓ Skipping UVM sub-stage '{stage_name}': checkpoint "
                f"found ({checkpoint_path(checkpoint_name)}), already "
                "completed in a previous run."
            )
            continue

        run_stage(llm, bash, build_prompt(), stage_name)
        mark_stage_done(checkpoint_name)

    # Hard gate: never allow Stage 5 simulation to run against a generated
    # TB whose directed-scenario mapping disagrees with the accepted plan.
    scenario_valid, scenario_errors = validate_scenario_manifest(PLAN, TB_DIR)
    if not scenario_valid:
        raise RuntimeError(
            "Generated scenario manifest validation failed after "
            "Stage 4 integration:\n"
            + "\n".join(f" - {error}" for error in scenario_errors)
        )

    print(
        "\n✓ cocotb+pyuvm environment generated directly under "
        f"/workspace/{TB_DIR_REL}."
    )
    print("✓ Directed scenario IDs validated against the accepted plan.")

    # Stage-4 generation is complete. Stage 5 diagnosis/repair and Stage
    # 6..9 improvement create their own sanitized agents. Release the
    # original full-workspace generation agent before any nested agent can
    # request the single `opencode_tools` resource.
    if bash is not None:
        bash.stop()

    # =================================================
    # Stage 5: Closed-loop generated-TB validation
    # =================================================
    # The generator owns verification intent. The simulator
    # owns execution. The repair LLM is only allowed to make
    # DUT/TB-specific changes after a structured diagnosis.

    if _tb_checkpoint_is_valid():
        print(
            "\n✓ Skipping TB validation and improvement: checkpoint found "
            f"({checkpoint_path('tb_validated')}) — the exact generated "
            "cocotb+pyuvm environment was already validated in a previous run."
        )
        print(
            "  No TB re-simulation, improvement iterations, or plateau detection "
            "will be performed."
        )
        run_rtl_verification_loop()
        return

    print("\n[5/5] Validating generated cocotb+pyuvm environment...")

    tb_dir = TB_DIR
    # Cocotb+pyUVM uses the benchmark-local generation_manifest.yaml directly.
    # The legacy tb_feedback manifest helper only understands SV-UVM output.

    final_analysis = None

    static_valid, static_errors = _validate_generated_tb_static(tb_dir)
    if not static_valid:
        raise RuntimeError(
            "Generated TB failed deterministic pre-simulation validation:\n"
            + "\n".join(f" - {error}" for error in static_errors)
        )
    print("✓ Generated TB passed deterministic pre-simulation validation.")

    for tb_attempt in range(MAX_TB_REPAIR_ATTEMPTS + 1):
        result_rel = (
            f"{DESIGN_GENERATED_ROOT}/results/tb_validation_"
            f"iteration_{tb_attempt}.json"
        )
        result_path = get(
            simulate.chia_remote(
                RTL,
                TB_DIR_REL,
                result_rel,
                SIM_TEST_TIMEOUT,
                SIM_BUILD_TIMEOUT,
            )
        )

        local_result_path = HOST_WORKSPACE / result_rel
        if not local_result_path.exists():
            raise RuntimeError(f"Simulation result was not created: {local_result_path}")
        final_analysis = analyze_results(str(local_result_path))
        print("\n===== TB SIMULATION ANALYSIS =====")
        print(json.dumps(final_analysis, indent=2))

        if final_analysis["verdict"] == "pass":
            print("\n✓ Generated cocotb+pyuvm environment passed simulation.")
            mark_stage_done(
                "tb_validated",
                detail={
                    "result_path": result_rel,
                    "tb_content_hash": _tb_content_hash(tb_dir),
                },
            )
            run_verification_improvement_loop(None, None, canonical_plan)
            # Unlike the two resume-path callers (checkpointed-TB and
            # non_actionable-diagnosis, above), this fresh-generation path
            # used to `return` here without ever reaching RTL verification --
            # a plateaued/complete improvement loop left rtl_verification_state.json
            # untouched, and since run_forever.sh stops on exit 0, nothing else
            # would ever call run_rtl_verification_loop() for this design.
            run_rtl_verification_loop()
            return

        if final_analysis["verdict"] == "template_bug":
            raise RuntimeError(
                "Template/toolchain defect detected. "
                "Do not patch this generated TB with the LLM. "
                "Fix the shared cocotb/pyuvm/Verilator templates or "
                "simulation infrastructure instead.\n\n"
                + json.dumps(final_analysis["detail"], indent=2)
            )

        if tb_attempt >= MAX_TB_REPAIR_ATTEMPTS:
            raise RuntimeError(
                "Generated cocotb+pyuvm environment still fails after "
                f"{MAX_TB_REPAIR_ATTEMPTS} repair attempts.\n\n"
                + json.dumps(final_analysis, indent=2)
            )

        # -------------------------------
        # Diagnosis LLM: failure -> plan
        # -------------------------------
        print(
            f"\n[DIAGNOSIS] Starting TB diagnosis "
            f"(attempt {tb_attempt + 1})...",
            flush=True,
        )
        diagnosis_workspace = HOST_WORKSPACE / DESIGN_GENERATED_ROOT / "llm_tb_diagnosis_workspace"
        print(
            f"[DIAGNOSIS] Workspace: {diagnosis_workspace}",
            flush=True,
        )
        diagnosis_report = {
            "source": "tb_validation",
            "result_path": str(local_result_path),
            "rtl_exposed_to_llm": False,
        }
        print("[DIAGNOSIS] Preparing sanitized workspace...", flush=True)
        prepare_sanitized_workspace(
            diagnosis_workspace,
            HOST_WORKSPACE / SPEC,
            HOST_WORKSPACE / REF_MODEL,
            HOST_WORKSPACE / PLAN,
            TB_DIR,
            diagnosis_report,
        )
        shutil.copy2(local_result_path, diagnosis_workspace / "simulation_result.json")
        print("[DIAGNOSIS] Sanitized inputs prepared; RTL remains excluded.", flush=True)
        diagnosis_llm, diagnosis_bash = create_improvement_agent(
            str(diagnosis_workspace),
            retries=LLM_RETRIES,
        )
        print(
            f"[DIAGNOSIS] LLM agent created (retries={LLM_RETRIES}).",
            flush=True,
        )
        try:
            print("[DIAGNOSIS] Building diagnosis prompt...", flush=True)
            diagnosis_prompt = _build_diagnosis_prompt(
                diagnosis_workspace,
                tb_attempt,
            )
            print(
                f"[DIAGNOSIS] Submitting prompt (length={len(diagnosis_prompt)} chars); waiting for LLM...",
                flush=True,
            )
            diagnosis_response = llm_get(
                diagnosis_llm.prompt.chia_remote(
                    diagnosis_llm, diagnosis_prompt, tools=[diagnosis_bash]
                )
            )
            print(
                f"[DIAGNOSIS] LLM response received (success={diagnosis_response.success}).",
                flush=True,
            )
        except Exception as exc:
            print(
                f"[DIAGNOSIS] LLM invocation raised {type(exc).__name__}: {exc}",
                flush=True,
            )
            raise
        finally:
            print("[DIAGNOSIS] Stopping diagnosis workspace agent...", flush=True)
            diagnosis_bash.stop()
            print("[DIAGNOSIS] Diagnosis workspace agent stopped.", flush=True)
        if not diagnosis_response.success:
            print(
                f"[DIAGNOSIS] LLM reported failure: {diagnosis_response.stderr}",
                flush=True,
            )
            # Must go through the shared helper: a bare
            # "TB diagnosis LLM failed:\n" with the empty stderr that
            # OpenCodeLLM.prompt() returns on a swallowed timeout matches no
            # marker, so the model is never cooled down and every supervisor
            # restart re-picks the same dead provider. Observed live
            # 2026-09-23: 35 identical failures, one every 17 seconds.
            _raise_on_llm_failure(diagnosis_response, "TB diagnosis")

        update_plan_path = (
            HOST_WORKSPACE
            / DESIGN_GENERATED_ROOT
            / "results"
            / f"tb_update_plan_iteration_{tb_attempt}.yaml"
        )
        # Never let a previous pipeline invocation become the
        # diagnosis for the current simulation result.
        if update_plan_path.exists():
            update_plan_path.unlink()
        if not update_plan_path.exists():
            print("[DIAGNOSIS] Converting LLM response into validated update plan...", flush=True)
            candidate_plan = ensure_valid_tb_update_plan_yaml(
                diagnosis_llm,
                diagnosis_response.result or "",
                max_attempts=5,
            )
            update_plan_path.parent.mkdir(parents=True, exist_ok=True)
            update_plan_path.write_text(candidate_plan)

        try:
            update_plan = yaml.safe_load(
                update_plan_path.read_text()
            )
        except yaml.YAMLError as exc:
            raise RuntimeError(
                f"Invalid TB update plan: {exc}"
            ) from exc

        print("[DIAGNOSIS] Validating update plan...", flush=True)
        valid_update_plan, update_plan_validation = validate_tb_update_plan(
            update_plan
        )
        if not valid_update_plan:
            raise RuntimeError(
                "Invalid TB update plan:\n"
                f"{update_plan_validation}"
            )
        update_plan = update_plan_validation

        print("[DIAGNOSIS] Update plan validated.", flush=True)
        print("\n===== TB UPDATE PLAN =====")
        print(yaml.safe_dump(update_plan, sort_keys=False))

        if update_plan.get("verdict") == "generation_bug":
            root_cause = update_plan.get("root_cause") or {}
            if not str(root_cause.get("summary", "")).strip() or not str(root_cause.get("evidence", "")).strip():
                raise RuntimeError(
                    "TB diagnosis returned generation_bug without concrete root-cause summary/evidence."
                )
            if not update_plan.get("changes"):
                raise RuntimeError(
                    "TB diagnosis returned generation_bug but proposed no generated-TB changes."
                )

        if update_plan.get("verdict") == "template_bug":
            raise RuntimeError(
                "TB diagnosis classified this as a "
                "template/toolchain issue. Fix the shared "
                "template/infrastructure rather than applying "
                "a per-generation repair.\n\n"
                + yaml.safe_dump(update_plan, sort_keys=False)
            )

        if update_plan.get("verdict") == "non_actionable":
            # This path is terminal for TB work. The diagnosis has established
            # that the observed failure is outside the generated TB (for example
            # a DUT functional defect). Do not enter the metric-driven TB
            # improvement loop: there is nothing actionable to optimize here,
            # so plateau detection would be meaningless and wasteful.
            tb_score_report = analyze_verification_state(
                PLAN,
                tb_dir,
                local_result_path,
                tb_attempt,
                None,
            )
            tb_score_path = (
                HOST_WORKSPACE
                / DESIGN_GENERATED_ROOT
                / "results"
                / f"tb_validation_score_iteration_{tb_attempt}.yaml"
            )
            tb_score_path.parent.mkdir(parents=True, exist_ok=True)
            tb_score_path.write_text(
                yaml.safe_dump(tb_score_report, sort_keys=False)
            )

            # Accept the exact current TB as final/validated. The content hash
            # makes this checkpoint reusable only while the TB remains unchanged.
            mark_stage_done(
                "tb_validated",
                detail={
                    "result_path": result_rel,
                    "tb_content_hash": _tb_content_hash(tb_dir),
                    "validation_verdict": "non_actionable",
                    "score_path": str(tb_score_path.relative_to(HOST_WORKSPACE)),
                },
            )

            print("\n===== TB SCORE (NON-ACTIONABLE) =====")
            print(
                f"TB quality score: "
                f"{float(tb_score_report.get('quality_score', 0.0)):.2f}"
            )
            print(
                "TB metrics: "
                + json.dumps(tb_score_report.get("metrics", {}), indent=2)
            )
            print(f"TB score report: {tb_score_path}")
            print(
                "\n✓ TB diagnosis classified the failure as non_actionable."
                "\n  The current generated TB is accepted as-is."
                "\n  No TB repair, improvement iterations, or plateau detection will be performed."
                "\n  Proceeding directly to RTL verification/repair."
            )
            run_rtl_verification_loop()
            return

        if update_plan.get("verdict") != "generation_bug":
            raise RuntimeError(
                "TB diagnosis did not produce a generation_bug "
                "repair plan.\n\n"
                + yaml.safe_dump(update_plan, sort_keys=False)
            )

        # -------------------------------
        # Repair LLM: plan -> TB changes
        # -------------------------------
        print(
            f"\nRepairing generated TB "
            f"(attempt {tb_attempt + 1}/"
            f"{MAX_TB_REPAIR_ATTEMPTS})..."
        )
        repair_workspace = HOST_WORKSPACE / DESIGN_GENERATED_ROOT / "llm_tb_repair_workspace"
        integrity_before = _verification_integrity_snapshot(tb_dir)
        repair_report = {
            "source": "tb_validation",
            "result": final_analysis,
            "repair_plan": update_plan,
            "rtl_exposed_to_llm": False,
        }
        prepare_sanitized_workspace(
            repair_workspace,
            HOST_WORKSPACE / SPEC,
            HOST_WORKSPACE / REF_MODEL,
            HOST_WORKSPACE / PLAN,
            tb_dir,
            repair_report,
        )
        shutil.copy2(update_plan_path, repair_workspace / "tb_update_plan.yaml")
        # Keep the normal improvement/diagnosis agent behavior unchanged.
        # TB repair gets one additional OpenCode retry because a transient
        # repair-agent timeout must not abort the entire TB validation loop.
        repair_llm, repair_bash = create_improvement_agent(
            str(repair_workspace),
            retries=LLM_RETRIES,
        )
        try:
            repair_response = llm_get(
                repair_llm.prompt.chia_remote(
                    repair_llm,
                    build_repair_prompt(
                        str(repair_workspace / "tb_update_plan.yaml"),
                        str(repair_workspace),
                    )
                    + """

REPAIR REQUIREMENTS:
- Repair ONLY the generated verification TB according to the diagnosis plan.
- Preserve verification intent and all valid checking behavior.
- Do NOT delete, disable, weaken, bypass, or comment out assertions/checkers.
- Do NOT remove scoreboard/reference-model comparisons or alter expected behavior
  to match the DUT.
- Do NOT delete coverage groups/bins or disable coverage merely to make simulation
  pass. For an API/runtime defect, fix the API usage while preserving coverage intent.
- Do NOT skip failing tests, suppress exceptions, or change stimulus solely to avoid
  the diagnosed failure.
- Do NOT modify RTL, specification, reference model, or verification plan.
- After editing, inspect the changed TB and ensure the diagnosed failure is actually
  addressed without reducing verification intent.
""",
                    tools=[repair_bash],
                )
            )
        finally:
            repair_bash.stop()
        if not repair_response.success:
            _raise_on_llm_failure(repair_response, "TB repair")

        changed = collect_tb_changes(repair_workspace / "tb", tb_dir)
        if not changed:
            raise RuntimeError("TB repair LLM completed without modifying the generated TB.")

        static_valid, static_errors = _validate_generated_tb_static(tb_dir)
        if not static_valid:
            raise RuntimeError(
                "TB repair failed deterministic static validation:\n"
                + "\n".join(f" - {error}" for error in static_errors)
            )

        integrity_after = _verification_integrity_snapshot(tb_dir)
        integrity_valid, integrity_errors = _validate_verification_integrity(
            integrity_before, integrity_after, changed
        )
        if not integrity_valid:
            raise RuntimeError(
                "TB repair failed verification-integrity validation:\n"
                + "\n".join(f" - {error}" for error in integrity_errors)
            )
        print("✓ TB repair passed static validation and verification-integrity checks.")

        # The generated environment is cocotb + pyUVM. Its
        # generation_manifest.yaml is validated by validate_scenario_manifest()
        # above; do not call the legacy SystemVerilog-UVM manifest helper here.
        manifest_valid, manifest_errors = validate_scenario_manifest(PLAN, tb_dir)
        if not manifest_valid:
            raise RuntimeError(
                "Generated scenario manifest became invalid after TB repair:\n"
                + "\n".join(f" - {error}" for error in manifest_errors)
            )
        print(repair_response.result.strip())

    raise RuntimeError(
        "TB validation loop ended unexpectedly: "
        + json.dumps(final_analysis, indent=2)
    )



# =========================================================
# Stages 6-9: metric-driven RTL-blind verification improvement
# =========================================================

def generate_llm_weakness_report(
    analysis_root,
    plan_path,
    tb_dir,
    simulation_result_path,
    deterministic_report,
):
    """Generate a compact RTL-blind weakness report from raw simulation logs.

    This is deliberately separate from the deterministic metric analyzer:
    Python measures objective results; the LLM interprets the raw evidence.
    """
    analysis_root = Path(analysis_root)
    simulation_result_path = Path(simulation_result_path)

    if analysis_root.exists():
        shutil.rmtree(analysis_root)

    prepare_sanitized_workspace(
        analysis_root,
        HOST_WORKSPACE / SPEC,
        HOST_WORKSPACE / REF_MODEL,
        plan_path,
        tb_dir,
        deterministic_report,
    )
    shutil.copy2(
        simulation_result_path,
        analysis_root / "simulation_result.json",
    )

    llm, bash = create_analysis_agent(str(analysis_root))
    try:
        response = llm_get(
            llm.prompt.chia_remote(
                llm,
                build_analysis_prompt(analysis_root),
                tools=[bash],
            )
        )
    finally:
        bash.stop()

    if not response.success:
        _raise_on_llm_failure(response, "verification analysis")

    raw_report = response.result or ""
    report_path = analysis_root / "weakness_report.yaml"

    # Prefer the file written by the analysis LLM, but accept a YAML response
    # as a compatibility fallback.
    if report_path.exists():
        report_text = report_path.read_text()
    else:
        report_text = raw_report

    # Apply the same clean -> parse -> validate -> repair boundary used by
    # the other LLM-authored YAML artifacts. This keeps a malformed analysis
    # response from aborting the verification-improvement loop while leaving
    # the diagnosis itself untouched.
    report_text = ensure_valid_weakness_report_yaml(
        llm,
        report_text,
        max_attempts=5,
    )

    try:
        parsed = yaml.safe_load(report_text)
    except yaml.YAMLError as exc:
        raise RuntimeError(
            "Verification analysis LLM returned invalid weakness-report YAML."
        ) from exc

    valid, error = validate_weakness_report(parsed)
    if not valid:
        raise RuntimeError(
            "Verification analysis LLM returned an invalid weakness report "
            "after YAML/schema repair:\n"
            f"{error}\n\n{report_text}"
        )

    # Persist the normalized, validated report in the analysis workspace so
    # the artifact and the returned object always have the same schema.
    report_path.write_text(
        yaml.safe_dump(parsed, sort_keys=False, allow_unicode=True)
    )

    # Keep only the compact report outside the analysis workspace.
    return parsed


def combine_verification_reports(deterministic_report, llm_report):
    """Combine objective metrics with LLM-diagnosed weaknesses."""
    combined = dict(deterministic_report)

    combined["weaknesses"] = list(llm_report.get("weaknesses", []))
    combined["analysis_summary"] = llm_report.get("summary", {})
    combined["authoritative_inputs"] = [
        "specification",
        "reference_model",
        "verification_plan",
    ]
    combined["rtl_exposed_to_llm"] = False

    # Raw simulation evidence remains in simulation_result.json for audit
    # purposes, but is intentionally not copied into the improvement report.
    combined.pop("simulation_evidence", None)

    return combined


def run_verification_improvement_loop(llm_unused, bash_unused, canonical_plan):
    """Iteratively improve the existing TB with crash-safe resume support.

    The accepted TB is treated as the durable state of the optimization.
    Every iteration runs against a snapshot of the last accepted TB. A
    candidate is either accepted and promoted to the durable snapshot, or
    rejected and rolled back. If this Python process dies at any point during
    an iteration, the next invocation restores the last accepted snapshot
    and retries that same iteration.

    The simulation worker sees RTL. The analysis and improvement LLMs never
    receive RTL or rtl_info.json.
    """
    tb_dir = TB_DIR
    plan_path = HOST_WORKSPACE / PLAN
    improvement_root = (
        HOST_WORKSPACE / DESIGN_GENERATED_ROOT / "llm_improvement_workspace"
    )
    analysis_root = (
        HOST_WORKSPACE / DESIGN_GENERATED_ROOT / "llm_verification_analysis_workspace"
    )
    reports_dir = (
        HOST_WORKSPACE / DESIGN_GENERATED_ROOT / "results"
        / "verification_improvement"
    )
    reports_dir.mkdir(parents=True, exist_ok=True)

    snapshots = HOST_WORKSPACE / DESIGN_GENERATED_ROOT / "tb_iterations"
    snapshots.mkdir(parents=True, exist_ok=True)
    accepted_snapshot = snapshots / "accepted"
    state_path = reports_dir / "improvement_state.json"
    history_path = reports_dir / "improvement_history.json"
    decisions_dir = reports_dir / "decisions"
    decisions_dir.mkdir(parents=True, exist_ok=True)

    if is_stage_done("verification_improvement_complete"):
        print("\n✓ Verification-improvement loop already completed.")
        return

    def atomic_write_json(path, payload):
        """Atomically persist small JSON state so crashes cannot leave a half-file."""
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True))
        os.replace(tmp, path)

    def save_state(**updates):
        """Persist improvement state after every durable transition."""
        state = {}
        if state_path.exists():
            try:
                state = json.loads(state_path.read_text())
            except Exception:
                # A corrupt/incomplete state file must never be trusted.
                # The accepted snapshot remains authoritative.
                state = {}
        state.update(updates)
        atomic_write_json(state_path, state)
        return state

    def restore_accepted_tb():
        """Restore the last durably accepted TB before resuming work."""
        if not accepted_snapshot.is_dir():
            raise RuntimeError(
                f"Accepted TB snapshot is missing: {accepted_snapshot}"
            )
        if tb_dir.exists():
            shutil.rmtree(tb_dir)
        shutil.copytree(accepted_snapshot, tb_dir)

    def persist_accepted_tb(iteration, report, plateau_attempts=0):
        """Make the current TB the durable accepted snapshot."""
        if not tb_dir.is_dir():
            raise RuntimeError(f"Cannot snapshot missing TB directory: {tb_dir}")
        tmp_snapshot = snapshots / f".accepted_tmp_{os.getpid()}"
        if tmp_snapshot.exists():
            shutil.rmtree(tmp_snapshot)
        shutil.copytree(
            tb_dir,
            tmp_snapshot,
            ignore=shutil.ignore_patterns(".chia_sim", "__pycache__", "*.so"),
        )
        if accepted_snapshot.exists():
            shutil.rmtree(accepted_snapshot)
        os.replace(tmp_snapshot, accepted_snapshot)
        save_state(
            accepted_iteration=iteration,
            next_iteration=iteration + 1,
            status="ready",
            quality_score=report.get("quality_score"),
            tests_failed=report.get("metrics", {}).get("tests_failed"),
            best_quality_score=(
                float(report.get("quality_score", 0.0))
                if report.get("quality_score") is not None
                else None
            ),
            plateau_attempts=plateau_attempts,
            accepted_snapshot=str(accepted_snapshot),
        )

    def persist_report(iteration, report):
        (reports_dir / f"iteration_{iteration}.yaml").write_text(
            yaml.safe_dump(report, sort_keys=False)
        )

    def record_improvement_history(entry):
        return append_improvement_history(history_path, entry)

    def plateau_reached(report, best_score, plateau_attempts):
        """Stop after the configured number of consecutive non-improving attempts.

        Plateau detection is intentionally score-based. Once the accepted
        verification environment has failed to improve the best quality score
        by at least PLATEAU_MIN_DELTA for PLATEAU_PATIENCE consecutive
        iterations, the improvement loop stops. Unresolved weaknesses may still
        be reported, but they do not allow an already-saturated improvement
        search to continue indefinitely.
        """
        return plateau_attempts >= PLATEAU_PATIENCE

    previous_report = None
    start_iteration = 1
    best_quality_score = None
    plateau_attempts = 0

    def _load_report(path):
        report = yaml.safe_load(path.read_text()) or {}
        if not isinstance(report, dict):
            raise RuntimeError(f"Invalid persisted improvement report: {path}")
        return report

    def _reconstruct_legacy_state():
        """
        Recover progress made by older run14 versions which did not persist
        improvement_state.json.  Existing iteration reports are treated as
        evidence of accepted progress when their explicit ``accepted`` flag
        is true.  The current TB is then snapshotted as the recovered accepted
        TB; this avoids guessing from old temporary snapshots.
        """
        report_files = []
        for path in reports_dir.glob("iteration_*.yaml"):
            try:
                number = int(path.stem.split("_")[-1])
            except ValueError:
                continue
            report_files.append((number, path))
        report_files.sort()

        if not report_files:
            return None, 1

        accepted_iteration = 0
        accepted_report = None
        baseline_path = reports_dir / "iteration_0.yaml"
        if baseline_path.exists():
            accepted_report = _load_report(baseline_path)

        # Only advance past iterations explicitly marked accepted.  This is
        # important because rejected candidates must not become the resume
        # baseline.  Reports without an accepted flag are considered legacy
        # candidates and are not guessed to be accepted.
        for number, path in report_files:
            if number == 0:
                continue
            report = _load_report(path)
            if report.get("accepted") is True:
                accepted_iteration = number
                accepted_report = report
            elif report.get("accepted") is False:
                continue

        if accepted_report is None:
            return None, 1

        # The old implementation kept tb_dir at the last accepted candidate
        # when it exited normally.  Snapshot that exact current TB instead of
        # trusting old iteration_* snapshot directories whose semantics varied
        # across versions.
        persist_accepted_tb(accepted_iteration, accepted_report)
        save_state(
            status="ready",
            current_iteration=None,
            accepted_iteration=accepted_iteration,
            next_iteration=accepted_iteration + 1,
            recovered_from_legacy_reports=True,
            recovered_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
        )
        return accepted_report, accepted_iteration + 1

    # ---------------------------------------------------------
    # Resume existing improvement state, or reconstruct state from reports
    # produced by older run14 versions.
    # ---------------------------------------------------------
    if state_path.exists() and accepted_snapshot.is_dir():
        try:
            state = json.loads(state_path.read_text())
        except Exception as exc:
            raise RuntimeError(
                f"Unable to read improvement state {state_path}: {exc}"
            ) from exc

        accepted_iteration = int(state.get("accepted_iteration", 0))
        start_iteration = int(state.get("next_iteration", accepted_iteration + 1))
        report_iteration = accepted_iteration
        report_path = reports_dir / f"iteration_{report_iteration}.yaml"

        if not report_path.exists():
            raise RuntimeError(
                "Improvement state references accepted iteration "
                f"{accepted_iteration}, but its report is missing: {report_path}"
            )

        previous_report = yaml.safe_load(report_path.read_text()) or {}
        if not isinstance(previous_report, dict):
            raise RuntimeError(f"Invalid persisted improvement report: {report_path}")
        best_quality_score = float(
            state.get("best_quality_score", previous_report.get("quality_score", 0.0))
        )
        plateau_attempts = int(state.get("plateau_attempts", 0))

        # Never trust a partially modified TB left by the crashed process.
        restore_accepted_tb()
        print(
            "\n↻ Resuming verification improvement: "
            f"last accepted iteration={accepted_iteration}, "
            f"next iteration={start_iteration}, "
            f"quality={float(previous_report.get('quality_score', 0.0)):.2f}"
        )

    else:
        # ---------------------------------------------------------
        # Legacy-state recovery before creating a new baseline.
        # ---------------------------------------------------------
        recovered_report, recovered_next = _reconstruct_legacy_state()
        if recovered_report is not None:
            previous_report = recovered_report
            start_iteration = recovered_next
            best_quality_score = float(previous_report.get("quality_score", 0.0))
            plateau_attempts = 0
            restore_accepted_tb()
            print(
                "\n↻ Recovered legacy verification-improvement progress: "
                f"last accepted iteration={recovered_next - 1}, "
                f"next iteration={recovered_next}, "
                f"quality={float(previous_report.get('quality_score', 0.0)):.2f}"
            )
        else:
            # ---------------------------------------------------------
            # Fresh baseline.
            # ---------------------------------------------------------
            baseline_result_rel = (
                f"{DESIGN_GENERATED_ROOT}/results/verification_improvement_baseline.json"
            )
            get(
                simulate.chia_remote(
                    RTL,
                    TB_DIR_REL,
                    baseline_result_rel,
                    SIM_TEST_TIMEOUT,
                    SIM_BUILD_TIMEOUT,
                )
            )
            baseline_path = HOST_WORKSPACE / baseline_result_rel

            deterministic_report = analyze_verification_state(
                plan_path,
                tb_dir,
                baseline_path,
                0,
                None,
            )

            print("\n===== RTL-BLIND VERIFICATION ANALYSIS =====")
            llm_report = generate_llm_weakness_report(
                analysis_root,
                plan_path,
                tb_dir,
                baseline_path,
                deterministic_report,
            )

            report = combine_verification_reports(
                deterministic_report,
                llm_report,
            )
            report["analysis_report_path"] = str(
                analysis_root / "weakness_report.yaml"
            )
            report["iteration"] = 0

            persist_report(0, report)
            previous_report = report
            best_quality_score = float(report.get("quality_score", 0.0))
            plateau_attempts = 0

            # Iteration 0 is the first durable accepted TB. Snapshot it BEFORE
            # writing the state that says the next iteration may begin.
            persist_accepted_tb(0, report)

            print("\n===== VERIFICATION BASELINE =====")
            print(yaml.safe_dump(report, sort_keys=False))

            start_iteration = 1

    # ---------------------------------------------------------
    # Improvement iterations.
    # ---------------------------------------------------------
    for iteration in range(start_iteration, MAX_IMPROVEMENT_ITERATIONS + 1):
        if best_quality_score is None:
            best_quality_score = float(previous_report.get("quality_score", 0.0))

        if plateau_reached(previous_report, best_quality_score, plateau_attempts):
            print(
                f"\n✓ Verification-improvement plateau reached after "
                f"{plateau_attempts} consecutive non-improving attempts "
                f"(minimum required improvement: {PLATEAU_MIN_DELTA:.2f})."
            )
            save_state(
                status="complete",
                current_iteration=None,
                next_iteration=iteration,
                best_quality_score=best_quality_score,
                plateau_attempts=plateau_attempts,
                stop_reason="quality_plateau",
                completed_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
            )
            mark_stage_done(
                "verification_improvement_complete",
                detail={
                    "iterations": iteration - 1,
                    "final_quality_score": previous_report.get("quality_score"),
                    "report_dir": str(reports_dir),
                    "stop_reason": "quality_plateau",
                },
            )
            return

        if not previous_report.get("weaknesses"):
            print("\n✓ No actionable weaknesses remain.")
            save_state(
                status="complete",
                next_iteration=iteration,
                completed_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
            )
            mark_stage_done(
                "verification_improvement_complete",
                detail={
                    "iterations": iteration - 1,
                    "final_quality_score": previous_report.get("quality_score"),
                    "report_dir": str(reports_dir),
                },
            )
            return

        # At the start of an iteration the accepted snapshot is authoritative.
        # This also makes a manual restart safe even if a stale candidate TB
        # is present in tb_dir.
        restore_accepted_tb()
        save_state(
            status="iteration_in_progress",
            current_iteration=iteration,
            next_iteration=iteration,
            started_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
        )

        snapshot = snapshots / f"iteration_{iteration - 1:02d}"
        if snapshot.exists():
            shutil.rmtree(snapshot)
        shutil.copytree(
            tb_dir,
            snapshot,
            ignore=shutil.ignore_patterns(
                ".chia_sim",
                "__pycache__",
                "*.so",
            ),
        )

        history = load_improvement_history(history_path)

        # Prepare a sanitized decision workspace containing the current TB,
        # evidence, advisory reasoning guidance, and prior experiments.
        prepare_sanitized_workspace(
            improvement_root,
            HOST_WORKSPACE / SPEC,
            HOST_WORKSPACE / REF_MODEL,
            plan_path,
            tb_dir,
            previous_report,
            reasoning_guidance_path=VERIFICATION_REASONING_GUIDANCE,
            improvement_history=history,
        )

        # ---------------------------------------------------------
        # Decision stage: diagnose/select intervention BEFORE any TB edit.
        # The decision is persisted and then treated as frozen input to the
        # execution LLM. The existing improvement workspace remains sanitized.
        # ---------------------------------------------------------
        decision_path = decisions_dir / f"iteration_{iteration}.yaml"
        decision_llm, decision_bash = create_improvement_agent(
            str(improvement_root),
            retries=LLM_RETRIES,
        )
        try:
            decision_response = llm_get(
                decision_llm.prompt.chia_remote(
                    decision_llm,
                    build_improvement_decision_prompt(improvement_root),
                    tools=[decision_bash],
                )
            )
        finally:
            decision_bash.stop()

        if not decision_response.success:
            _raise_on_llm_failure(decision_response, "verification improvement decision")

        decision = save_improvement_decision(
            decision_path,
            decision_response.result or "",
        )

        print(f"\n===== IMPROVEMENT DECISION (iteration {iteration}) =====")
        print(yaml.safe_dump(decision, sort_keys=False))

        action = decision.get("decision", {}).get("action", "defer")

        if action in {"no_change", "defer"}:
            # No TB modification is attempted. This is a completed experiment
            # and the accepted snapshot remains authoritative.
            record_improvement_history({
                "iteration": iteration,
                "weakness_id": decision.get("decision", {}).get("weakness_id", ""),
                "failure_class": decision.get("decision", {}).get("failure_class", ""),
                "failure_mechanism": decision.get("decision", {}).get("failure_mechanism", ""),
                "action": action,
                "source": decision.get("decision", {}).get("source", ""),
                "confidence": decision.get("decision", {}).get("confidence", ""),
                "selected_intervention": decision.get("selected_intervention"),
                "result": "no_change",
                "reason": decision.get("diagnosis", {}).get("observed_problem", ""),
                "decision_path": str(decision_path),
            })

            no_change_report = dict(previous_report)
            no_change_report["accepted"] = False
            no_change_report["changed_files"] = []
            no_change_report["iteration_status"] = action
            no_change_report["decision_path"] = str(decision_path)
            persist_report(iteration, no_change_report)

            plateau_attempts += 1
            save_state(
                status="ready",
                accepted_iteration=int(previous_report.get("iteration", iteration - 1)),
                next_iteration=iteration + 1,
                current_iteration=None,
                last_result=action,
                best_quality_score=best_quality_score,
                plateau_attempts=plateau_attempts,
            )
            print(
                f"\n• Improvement iteration {iteration}: "
                f"decision={action}; retaining accepted TB and continuing."
            )
            continue

        # ---------------------------------------------------------
        # Execution stage: modify TB according to the frozen decision.
        # ---------------------------------------------------------
        prepare_sanitized_workspace(
            improvement_root,
            HOST_WORKSPACE / SPEC,
            HOST_WORKSPACE / REF_MODEL,
            plan_path,
            tb_dir,
            previous_report,
            reasoning_guidance_path=VERIFICATION_REASONING_GUIDANCE,
            improvement_history=history,
            decision=decision,
        )

        llm, bash = create_improvement_agent(str(improvement_root))
        try:
            response = llm_get(
                llm.prompt.chia_remote(
                    llm,
                    build_improvement_prompt(
                        improvement_root,
                        decision=decision,
                    ),
                    tools=[bash],
                )
            )
            if not response.success:
                _raise_on_llm_failure(response, "verification improvement")
            changed = collect_tb_changes(
                improvement_root / "tb",
                tb_dir,
            )
        finally:
            bash.stop()

        if not changed:
            # A no-op is a completed iteration, not a fatal process error.
            # Keep the accepted TB and continue trying future iterations.
            no_change_report = dict(previous_report)
            no_change_report["accepted"] = False
            no_change_report["changed_files"] = []
            no_change_report["iteration_status"] = "no_change"
            no_change_report["decision_path"] = str(decision_path)
            persist_report(iteration, no_change_report)

            record_improvement_history({
                "iteration": iteration,
                "weakness_id": decision.get("decision", {}).get("weakness_id", ""),
                "failure_class": decision.get("decision", {}).get("failure_class", ""),
                "failure_mechanism": decision.get("decision", {}).get("failure_mechanism", ""),
                "action": "improve",
                "source": decision.get("decision", {}).get("source", ""),
                "confidence": decision.get("decision", {}).get("confidence", ""),
                "selected_intervention": decision.get("selected_intervention"),
                "result": "no_change",
                "reason": "Execution LLM produced no TB changes.",
                "decision_path": str(decision_path),
            })

            print(
                f"\n• Improvement iteration {iteration}: "
                "decision was improve but LLM made no changes; "
                "retaining accepted TB and continuing."
            )
            plateau_attempts += 1
            save_state(
                status="ready",
                accepted_iteration=int(previous_report.get("iteration", iteration - 1)),
                next_iteration=iteration + 1,
                current_iteration=None,
                last_result="no_change",
                best_quality_score=best_quality_score,
                plateau_attempts=plateau_attempts,
            )
            continue

        # Candidate regression.
        candidate_result_rel = (
            f"{DESIGN_GENERATED_ROOT}/results/"
            f"verification_improvement_candidate_{iteration}.json"
        )
        get(
            simulate.chia_remote(
                RTL,
                TB_DIR_REL,
                candidate_result_rel,
                SIM_TEST_TIMEOUT,
                SIM_BUILD_TIMEOUT,
            )
        )
        candidate_path = HOST_WORKSPACE / candidate_result_rel

        # Candidate evaluation is intentionally deterministic first.
        # Do NOT generate a new RTL-blind weakness report yet: the candidate
        # may be rejected, in which case its diagnosis would describe a TB
        # that is immediately discarded.
        deterministic_candidate_report = analyze_verification_state(
            plan_path,
            tb_dir,
            candidate_path,
            iteration,
            previous_report,
        )

        old_score = float(previous_report.get("quality_score", 0.0))
        new_score = float(deterministic_candidate_report.get("quality_score", 0.0))
        old_failed = int(
            previous_report.get("metrics", {}).get("tests_failed", 0)
        )
        new_failed = int(
            deterministic_candidate_report.get("metrics", {}).get("tests_failed", 0)
        )

        accepted = (
            new_score >= old_score
            and new_failed <= old_failed
        )

        print(
            f"\n===== CANDIDATE EVALUATION "
            f"(iteration {iteration}) ====="
        )
        print(
            f"Old score: {old_score:.2f} | "
            f"Candidate score: {new_score:.2f} | "
            f"Old failed: {old_failed} | "
            f"Candidate failed: {new_failed}"
        )

        # ---------------------------------------------------------
        # REJECTED CANDIDATE
        # ---------------------------------------------------------
        # Keep the existing accepted weakness report unchanged. There is no
        # reason to spend an LLM analysis call on a TB that will be discarded.
        if not accepted:
            candidate_report = dict(deterministic_candidate_report)
            candidate_report["accepted"] = False
            candidate_report["changed_files"] = changed
            candidate_report["decision_path"] = str(decision_path)
            candidate_report["iteration_status"] = "rejected"
            persist_report(iteration, candidate_report)

            record_improvement_history({
                "iteration": iteration,
                "weakness_id": decision.get("decision", {}).get("weakness_id", ""),
                "failure_class": decision.get("decision", {}).get("failure_class", ""),
                "failure_mechanism": decision.get("decision", {}).get("failure_mechanism", ""),
                "action": "improve",
                "source": decision.get("decision", {}).get("source", ""),
                "confidence": decision.get("decision", {}).get("confidence", ""),
                "selected_intervention": decision.get("selected_intervention"),
                "changed_files": changed,
                "result": "rejected",
                "old_quality_score": old_score,
                "new_quality_score": new_score,
                "old_tests_failed": old_failed,
                "new_tests_failed": new_failed,
                "decision_path": str(decision_path),
                "reason": "Candidate did not satisfy existing acceptance criteria.",
                "weakness_report_generated": False,
            })

            restore_accepted_tb()
            plateau_attempts += 1
            save_state(
                status="ready",
                current_iteration=None,
                next_iteration=iteration + 1,
                last_result="rejected",
                last_candidate_score=new_score,
                best_quality_score=best_quality_score,
                plateau_attempts=plateau_attempts,
            )
            print(
                f"\n✗ Rejected TB improvement iteration {iteration}: "
                f"{old_score:.2f} -> {new_score:.2f}; "
                "restored last accepted TB and continuing with the "
                "existing weakness report."
            )
            # previous_report remains the accepted report.
            continue

        # ---------------------------------------------------------
        # ACCEPTED CANDIDATE
        # ---------------------------------------------------------
        # Only now is this TB the new verification state. Generate the new
        # weakness report exactly like the initial baseline analysis, using
        # the accepted candidate TB and its simulation evidence.
        print(
            f"\n✓ Candidate satisfied acceptance criteria for iteration "
            f"{iteration}: {old_score:.2f} -> {new_score:.2f}"
        )
        print(
            f"\n===== RTL-BLIND VERIFICATION ANALYSIS "
            f"(accepted iteration {iteration}) ====="
        )

        accepted_llm_report = generate_llm_weakness_report(
            analysis_root,
            plan_path,
            tb_dir,
            candidate_path,
            deterministic_candidate_report,
        )

        candidate_report = combine_verification_reports(
            deterministic_candidate_report,
            accepted_llm_report,
        )
        candidate_report["accepted"] = True
        candidate_report["changed_files"] = changed
        candidate_report["analysis_report_path"] = str(
            analysis_root / "weakness_report.yaml"
        )
        candidate_report["decision_path"] = str(decision_path)
        candidate_report["iteration"] = iteration
        candidate_report["iteration_status"] = "accepted"

        # The new weakness report is now the diagnosis for the newly accepted
        # TB and therefore becomes the input to the next improvement attempt.
        previous_report = candidate_report

        # Plateau is measured against the best accepted score, not merely
        # against the immediately preceding candidate.
        if new_score >= best_quality_score + PLATEAU_MIN_DELTA:
            best_quality_score = new_score
            plateau_attempts = 0
        else:
            plateau_attempts += 1

        # Persist the accepted report and TB only after the complete
        # deterministic + RTL-blind analysis state is available. The
        # accepted report remains a valid schema-validated weakness report
        # because generate_llm_weakness_report() applies its dedicated
        # clean -> parse -> validate -> repair boundary.
        persist_report(iteration, previous_report)
        persist_accepted_tb(
            iteration,
            previous_report,
            plateau_attempts=plateau_attempts,
        )
        save_state(
            status="ready",
            accepted_iteration=iteration,
            next_iteration=iteration + 1,
            current_iteration=None,
            best_quality_score=best_quality_score,
            plateau_attempts=plateau_attempts,
            last_result="accepted",
        )

        record_improvement_history({
            "iteration": iteration,
            "weakness_id": decision.get("decision", {}).get("weakness_id", ""),
            "failure_class": decision.get("decision", {}).get("failure_class", ""),
            "failure_mechanism": decision.get("decision", {}).get("failure_mechanism", ""),
            "action": "improve",
            "source": decision.get("decision", {}).get("source", ""),
            "confidence": decision.get("decision", {}).get("confidence", ""),
            "selected_intervention": decision.get("selected_intervention"),
            "changed_files": changed,
            "result": "accepted",
            "old_quality_score": old_score,
            "new_quality_score": new_score,
            "old_tests_failed": old_failed,
            "new_tests_failed": new_failed,
            "decision_path": str(decision_path),
            "reason": "Candidate satisfied existing acceptance criteria.",
            "weakness_report_generated": True,
        })

        print(
            f"\n✓ Accepted TB improvement iteration {iteration}: "
            f"{old_score:.2f} -> {new_score:.2f}"
        )

    # Reaching the configured maximum is a normal completion, not a failure.
    final_iteration = MAX_IMPROVEMENT_ITERATIONS
    save_state(
        status="complete",
        current_iteration=None,
        next_iteration=final_iteration + 1,
        best_quality_score=best_quality_score,
        plateau_attempts=plateau_attempts,
        stop_reason="max_iterations",
        completed_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
    )
    mark_stage_done(
        "verification_improvement_complete",
        detail={
            "iterations": final_iteration,
            "final_quality_score": (
                previous_report.get("quality_score")
                if previous_report
                else None
            ),
            "report_dir": str(reports_dir),
        },
    )



# =========================================================
# Stage 10 — RTL verification + RTL repair loop
# =========================================================

def validate_rtl_repair_decision(decision):
    """Validate the RTL-repair LLM contract before any RTL edit."""
    if not isinstance(decision, dict):
        return False, "RTL repair decision must be a YAML mapping."
    if str(decision.get("schema_version", "")).strip() != "1.0":
        return False, 'schema_version must be "1.0".'
    action = decision.get("action")
    if action not in {"repair", "no_repair", "template_bug", "non_actionable"}:
        return False, "action must be repair, no_repair, template_bug, or non_actionable."
    if not isinstance(decision.get("root_cause"), str) or not decision["root_cause"].strip():
        return False, "root_cause must be a non-empty string."
    if not isinstance(decision.get("evidence"), list):
        return False, "evidence must be a list."
    if not isinstance(decision.get("confidence"), str) or decision["confidence"] not in {"high", "medium", "low"}:
        return False, "confidence must be high, medium, or low."
    changes = decision.get("changes", [])
    if not isinstance(changes, list):
        return False, "changes must be a list."
    for i, change in enumerate(changes):
        if not isinstance(change, dict):
            return False, f"changes[{i}] must be a mapping."
        if not isinstance(change.get("file"), str) or not change["file"].strip():
            return False, f"changes[{i}].file must be a non-empty string."
        path = Path(change["file"])
        if path.is_absolute() or ".." in path.parts:
            return False, f"changes[{i}].file must be a safe relative path."
        if change.get("action") not in {"modify", "add", "delete"}:
            return False, f"changes[{i}].action must be modify, add, or delete."
        if not isinstance(change.get("instructions"), list):
            return False, f"changes[{i}].instructions must be a list."
    return True, decision


def ensure_valid_rtl_repair_decision_yaml(llm, text, max_attempts=5):
    """Clean, parse, validate, and minimally repair RTL-repair decisions."""
    candidate = clean_yaml_response(text)
    last_error = None
    for attempt in range(max_attempts + 1):
        try:
            parsed = yaml.safe_load(candidate)
        except yaml.YAMLError as exc:
            parsed = None
            error = f"Invalid YAML: {exc}"
        else:
            valid, result = validate_rtl_repair_decision(parsed)
            if valid:
                return yaml.safe_dump(result, sort_keys=False, allow_unicode=True)
            error = result
        last_error = error
        if attempt >= max_attempts:
            break
        prompt = f"""
You are a strict YAML/schema repair tool for an RTL-repair decision.
Repair ONLY syntax/schema conformance. Preserve the diagnosis, evidence,
root cause, action, and intended RTL changes. Do not invent changes.
Return ONLY the complete YAML document.

Required structure:
schema_version: "1.0"
action: repair | no_repair | template_bug | non_actionable
root_cause: <short root cause>
evidence:
  - <specific evidence>
confidence: high | medium | low
changes:
  - file: <safe RTL-relative path>
    action: modify | add | delete
    instructions:
      - <concrete change>

Parser/validation error:
{last_error}

Malformed document:
{candidate}
"""
        response = get(llm.prompt.chia_remote(llm, prompt, tools=[]))
        if not response.success:
            raise RuntimeError(f"RTL repair-decision YAML repair failed:\n{response.stderr}")
        candidate = clean_yaml_response(response.result or "")
        if not candidate:
            raise RuntimeError("RTL repair-decision YAML repair returned an empty response.")
    raise RuntimeError(f"Unable to produce a valid RTL repair decision after {max_attempts} attempts: {last_error}")


# Outcomes that mean "this iteration's diagnosis did not lead to a fix" --
# worth warning a later iteration about so it doesn't re-derive the same
# wrong theory from scratch. "accepted" is deliberately excluded: an
# accepted candidate is already reflected in the current accepted RTL
# snapshot, so there's nothing further to warn about.
_RTL_HISTORY_UNRESOLVED_RESULTS = {
    "rejected", "no_change", "non_actionable", "template_bug",
    "ungrounded_reference",
}


def format_prior_iterations_context(history, current_iteration):
    """Summarize earlier same-run RTL-repair attempts that did NOT resolve
    the failure, for inclusion in a later iteration's diagnosis prompt.

    Without this, each iteration's analysis workspace is rebuilt from
    scratch with no memory of previous attempts (see
    build_rtl_failure_analysis_prompt) -- a diagnosis can re-derive and
    re-propose the exact same ungrounded theory every iteration, since
    nothing tells it that theory was already tried and didn't help.
    """
    prior = [
        entry for entry in history
        if isinstance(entry, dict)
        and entry.get("iteration") is not None
        and entry["iteration"] < current_iteration
        and entry.get("result") in _RTL_HISTORY_UNRESOLVED_RESULTS
    ]
    if not prior:
        return ""

    lines = [
        f"- Iteration {entry['iteration']} ({entry['result']}): "
        f"{str(entry.get('root_cause', '')).strip() or '(no root_cause recorded)'}"
        for entry in prior
    ]
    return (
        "PRIOR ATTEMPTS IN THIS SAME VERIFICATION RUN (none of these fixed "
        "the failure):\n" + "\n".join(lines) + "\n\n"
        "These are provided so you do not repeat a diagnosis that already "
        "failed to fix the problem. Before citing any specific line number "
        "or line content as evidence, re-read rtl.sv directly in this "
        "workspace and quote only what is actually there -- do not assume "
        "a previous iteration's description of the file is still accurate "
        "(it may have been wrong, or the file may differ from what an "
        "earlier attempt claimed)."
    )


# Two alternative phrasings observed in practice:
#   "replace/change [the X] `old` [at line N] with/to `new`"
#   "... from `old` to `new` ..."
# Anchoring the second pattern on "from ... to" (rather than reusing the
# first pattern's "replace/change" prefix) matters: a sentence like
# "change the assignment inside `if (round == 4'd9)` branch from `done
# <= 1'b0;` to `done <= 1'b1;`" has an unrelated quoted span (the
# condition) between "change" and the actual old/new pair, which would
# otherwise be mismatched as the "old" text.
_RTL_CHANGE_PAIR_RES = [
    re.compile(
        r"""
        (?:replace|change)\s+               # replace/change ...
        (?:(?:the|line)\b[^`"]*?)?          # optional "the X" / "line 152" filler
        [`"]([^`"]+)[`"]                    # old text, quoted
        [^`"]*?                             # optional filler, e.g. " at line 152 "
        \s+(?:with|to)\s+                   # with/to
        [`"]([^`"]+)[`"]                    # new text, quoted
        """,
        re.IGNORECASE | re.VERBOSE,
    ),
    re.compile(
        r"""
        \bfrom\s+
        [`"]([^`"]+)[`"]                    # old text, quoted
        \s+to\s+
        [`"]([^`"]+)[`"]                    # new text, quoted
        """,
        re.IGNORECASE | re.VERBOSE,
    ),
]


def _normalize_rtl_snippet(text):
    # Strip an optional leading "NNN:" line-number prefix (seen in some
    # decisions, e.g. "152:             done <= 1'b1;") and collapse
    # whitespace so formatting differences don't cause false mismatches.
    text = re.sub(r"^\s*\d+\s*:\s*", "", text.strip())
    return " ".join(text.split())


def find_ungrounded_rtl_changes(decision, current_rtl_text):
    """Check whether a repair decision's cited "old" snippets actually
    appear in the current RTL, before spending a repair-apply LLM call and
    a full simulation on it.

    Deliberately conservative: only flags an instruction when it matches
    one of the "replace/change X with/to Y" phrasings this pipeline's
    decisions consistently use (see build_rtl_repair_prompt's examples in
    practice) AND the quoted old text is genuinely absent. An instruction
    that doesn't match the pattern is skipped, never treated as a problem
    -- false negatives are fine here, false positives would block valid
    repairs phrased slightly differently.
    """
    normalized_current = " ".join(current_rtl_text.split())
    problems = []
    for change in decision.get("changes") or []:
        if not isinstance(change, dict) or change.get("action") not in {"modify", "delete"}:
            continue
        if str(change.get("file", "")).strip() not in {"rtl.sv", "", None}:
            continue
        for instruction in change.get("instructions") or []:
            if not isinstance(instruction, str):
                continue
            seen_pairs = set()
            for pattern in _RTL_CHANGE_PAIR_RES:
                for old_snippet, _new_snippet in pattern.findall(instruction):
                    if old_snippet in seen_pairs:
                        continue
                    seen_pairs.add(old_snippet)
                    old_norm = _normalize_rtl_snippet(old_snippet)
                    if old_norm and old_norm not in normalized_current:
                        problems.append(
                            f'cited old text {old_snippet!r} was not found in the '
                            f'current rtl.sv (instruction: {instruction!r})'
                        )
    return problems


def build_rtl_failure_analysis_prompt(workspace, retry_context=None, prior_iterations_context=None):
    retry_context = retry_context or ""
    prior_iterations_context = prior_iterations_context or ""
    retry_section = ""
    if retry_context.strip():
        retry_section = f"""

PREVIOUS NON_ACTIONABLE DIAGNOSIS CONTEXT:
The previous diagnosis in this SAME RTL iteration returned non_actionable.
That means the previous analysis did not establish enough evidence for a
grounded repair decision. It does NOT mean that the RTL is unrepairable.

Use the additional context below to re-evaluate the failure:
---BEGIN PREVIOUS CONTEXT---
{retry_context}
---END PREVIOUS CONTEXT---

RETRY RULES:
- Re-evaluate the failure from the available evidence; do not merely repeat
  the previous conclusion.
- Use the previous evidence to identify what was missing or uncertain.
- If the evidence now supports a concrete RTL defect, return action=repair
  with the smallest evidence-grounded change.
- If the evidence still genuinely does not support a safe RTL conclusion,
  return action=non_actionable again.
- Do NOT return non_actionable merely because an earlier attempt did.
- Do NOT invent a repair just because this is a retry.
- Do NOT treat previous failed repair attempts as proof that the RTL is
  unrepairable.
"""
    prior_section = ""
    if prior_iterations_context.strip():
        prior_section = f"\n\n{prior_iterations_context}"
    return f"""
You are the RTL verification failure-analysis LLM.

Work ONLY in the current workspace. The workspace contains:
- rtl.sv: the current accepted RTL
- specification.md
- reference_model.py
- verification_plan.yaml
- simulation_result.json
- tb/: the frozen accepted cocotb + pyuvm environment

Unlike the TB-improvement loop, RTL is intentionally visible to you here.
The purpose of this stage is to determine whether the observed failure is
actually caused by the RTL, and if so, identify the smallest RTL repair.
{retry_section}{prior_section}
IMPORTANT:
- The cocotb + pyuvm TB is FROZEN. Do not propose TB changes.
- Do not modify the specification, reference model, verification plan, or TB.
- Treat simulation_result.json as the authoritative execution evidence.
- Compare failing behavior against BOTH the specification and reference model.
- Distinguish an RTL defect from a TB/infrastructure defect.
- Do not call a behavior an RTL bug merely because RTL and specification differ;
  first determine which source defines intended behavior and record a discrepancy
  when appropriate.
- If the failure is caused by the TB, toolchain, or infrastructure, use
  action=no_repair or action=template_bug as appropriate.
- Do not invent an RTL change that is not supported by evidence.
- Prefer the smallest behavior-preserving correction that fixes the demonstrated
  failure without changing unrelated functionality.

## WHAT EACH ACTION MEANS — pick carefully, this decision is not reviewed
## by anyone before it takes effect
- `repair`: you have identified a specific RTL defect that explains the
  observed failure(s), and you can name the exact line(s)/logic to change.
- `no_repair`: you are asserting, with evidence, that the RTL is CORRECT
  and the accepted RTL should be considered done. This is a strong claim
  — it means "ship this RTL as final." Only choose it when you can show,
  for the specific tests that failed, that the failure is caused by the
  TB/environment and not by the RTL. Never choose `no_repair` just
  because you cannot find the bug, cannot explain a failure, or because
  simulation_result.json's failures look generic/uninformative — that is
  `non_actionable`, not `no_repair`.
- `template_bug`: the failure is caused by shared toolchain/template
  infrastructure (e.g. wrong module name expected by the harness, a
  simulator/build defect) that no per-design RTL or TB change can fix.
- `non_actionable`: you do not have enough evidence to safely choose
  `repair` or `no_repair`. This is the SAFE default when evidence is
  thin, contradictory, or the simulation itself did not produce real
  pass/fail results (see below). Choosing `non_actionable` costs nothing
  — the pipeline automatically retries the analysis with a fresh look.
  Choosing `no_repair` incorrectly ships unverified RTL as if it were
  correct. When genuinely unsure, prefer `non_actionable`.

## HARD RULE — when you must NOT choose no_repair
If simulation_result.json shows `status` other than a normal completed
run (e.g. `validation_error`, `build_failure`, a crash, or every single
test failing with the same generic/infrastructure-looking error and zero
tests showing a real pass/fail comparison), you have ZERO functional
test evidence about the RTL's correctness. In that situation `no_repair`
is never justified — use `non_actionable` (if you suspect it might still
be fixable with different evidence) or `template_bug` (if the cause is
clearly toolchain/harness-level, e.g. a missing/misnamed generated
file). Declaring "the RTL must be fine because the failures look
TB-related" without being able to point to a SPECIFIC TB defect is not
evidence — it is a guess, and guesses must map to `non_actionable`.

## EVIDENCE BAR (applies to every action)
Each `evidence` item must satisfy BOTH of these, not just one:
1. Cite a SPECIFIC RTL line/signal/behavior (quote it or give the exact
   line number) — not a general description of the module.
2. Cite the SPECIFIC test in simulation_result.json it explains (test
   name + its actual reported status/error) — not "the tests fail" in
   general.
A vague evidence item that does neither is worthless and will be
rejected downstream. If you choose `no_repair`, your evidence must
collectively account for EVERY test that failed in simulation_result.json
— not just the easiest one or two to explain. If you cannot do that for
all of them, you do not have enough evidence for `no_repair`; use
`non_actionable` instead.

Return ONLY YAML:

schema_version: "1.0"
action: repair | no_repair | template_bug | non_actionable
root_cause: <specific root cause>
evidence:
  - <RTL line/behavior AND the specific simulation_result.json test it explains>
confidence: high | medium | low
changes:
  - file: <RTL-relative path, normally rtl.sv>
    action: modify
    instructions:
      - <smallest concrete repair>

For action=no_repair, template_bug, or non_actionable, changes may be [].

## BEFORE YOU ANSWER
- If action is `no_repair`: does your evidence cover EVERY failing test
  in simulation_result.json, with a specific reason per test? If not,
  change action to `non_actionable`.
- If action is `repair`: can you point to the exact line(s) in rtl.sv
  that are wrong, and state what they currently do vs. what they should
  do? If not, you do not have enough evidence for `repair` either.
- Is every `evidence` item specific (a line/signal AND a named test), or
  did you write a general summary sentence? Rewrite any general sentence
  into a specific, checkable claim.

Do not include markdown fences or explanations outside the YAML.
""".strip()


def build_rtl_repair_prompt(workspace):
    return f"""
You are the RTL repair LLM.

Work in the current workspace. Read:
- rtl.sv (the current accepted RTL)
- rtl_repair_decision.yaml
- simulation_result.json
- specification.md
- reference_model.py
- verification_plan.yaml
- tb/ (read-only frozen verification environment)

Apply ONLY the repair described in rtl_repair_decision.yaml.

HARD RULES:
- Modify ONLY rtl.sv.
- Never modify tb/, specification.md, reference_model.py, or verification_plan.yaml.
- Do not weaken the testbench or disable assertions/checks to make the run pass.
- Do not change interfaces, ports, widths, or parameters unless the frozen
  evidence and repair decision explicitly require it.
- Do not rewrite unrelated RTL.
- Preserve all unrelated behavior.
- The repair must address the diagnosed root cause, not merely suppress the
  observed error.
- Apply EXACTLY the instructions in rtl_repair_decision.yaml — do not use
  it as inspiration to make a broader change you think is "really" needed.
  If you believe the decision's instructions are insufficient or wrong
  once you look at the actual RTL, make the smallest change that still
  satisfies the decision's stated intent; do not silently expand scope.

BEFORE EDITING, for the specific line(s) you are about to change, write
out (in your own working notes, not the final summary) what that line
currently does and what it should do instead per the repair decision.
If you cannot state both clearly, re-read rtl_repair_decision.yaml and
the cited evidence before touching any code — do not guess.

AFTER EDITING:
- Inspect rtl.sv for syntax, width, reset, clocking, and unintended
  behavioral changes.
- Confirm every line you changed matches an instruction in
  rtl_repair_decision.yaml — if a line changed that isn't traceable to
  an instruction, revert it.
- Confirm you did not touch any signal/port/parameter not named in the
  repair decision.

Return a concise summary only, stating for each change: the line(s)
touched, what they did before, and what they do now. Do not paste the
RTL source.
""".strip()


def _candidate_rtl_built(analysis: dict) -> bool:
    """False when the candidate RTL never compiled.

    A build failure makes every test exit with returncode 2 before it runs, so
    the candidate's quality score comes out identical to the baseline's -- which
    the score-only non-regression check below reads as "no regression" and
    promotes. Accepting RTL that does not compile poisons every later iteration,
    because each one starts from the accepted snapshot (observed live: an S-box
    repair emitted C-style 0x77 literals into SystemVerilog and was promoted,
    after which no iteration could make progress).

    returncode 2 alone does NOT mean a build failure, which an earlier version
    of this gate assumed. cocotb's make target also exits 2 when the design
    builds fine and a test simply FAILS -- confirmed live on
    aes128_benchmark_corrupted, whose log reads "TESTS=1 PASS=0 FAIL=1" and
    "Verilog $finish" with returncode 2 on every test. Since that design is
    deliberately fault-injected, failing tests are its expected starting point,
    and a returncode-only gate would have rejected every RTL repair as
    "failed to build" and stalled the loop permanently.

    The discriminator is whether a test produced its results file: a test that
    ran writes one, a test whose build failed never gets that far. So treat the
    candidate as un-built only when NOTHING ran. Anything else falls through to
    the existing score comparison.
    """
    detail = analysis.get("detail") or {}
    # analyze_results() nests `detail` two different ways. For a functional
    # failure it is the raw simulation data, so tests sit at detail["tests"];
    # for validation_error / build_failure it is
    # {"status", "category", "data"} and the tests sit at
    # detail["data"]["tests"]. Reading only the first shape meant this gate
    # saw an empty dict and returned True -- "built" -- for exactly the two
    # statuses that mean the design did NOT build.
    tests = detail.get("tests")
    if tests is None:
        tests = (detail.get("data") or {}).get("tests")
    if detail.get("status") in {"validation_error", "build_failure"}:
        return False
    tests = tests or {}
    if not tests:
        return True
    # If any test produced results, the design compiled -- whatever it then did.
    if any(t.get("results_file_exists") for t in tests.values()):
        return True
    return not all(t.get("returncode") == 2 for t in tests.values())


def run_rtl_verification_loop():
    """Verify the frozen accepted TB against RTL without modifying benchmark RTL.

    The benchmark RTL at ``RTL`` is immutable input.  A durable copy is created
    under ``generated/designs/<design>/rtl_verification/accepted/`` and every
    simulation uses either that accepted snapshot or an isolated candidate.
    A candidate that passes is promoted immediately and completes verification.
    A failing candidate is promoted whenever it does not regress the
    deterministic verification quality score versus the currently accepted
    RTL (score stays the same or improves). This allows incremental RTL
    repair across iterations - a candidate that fixes a real bug but leaves
    the score unchanged (because other, still-unfixed bugs mask the effect)
    is kept rather than discarded, so the LLM is not forced to rediscover
    the same bug from scratch on the next iteration. Cross-iteration history
    (``rtl_repair_history.json``) is fed back into the diagnosis prompt so
    the LLM knows what has already been tried. A candidate is only rejected
    when it actually makes the quality score worse. The benchmark RTL is
    never modified.
    """
    tb_dir = TB_DIR
    plan_path = HOST_WORKSPACE / PLAN
    original_rtl_path = HOST_WORKSPACE / RTL

    root = HOST_WORKSPACE / DESIGN_GENERATED_ROOT / "rtl_verification"
    iterations_dir = root / "iterations"
    accepted_dir = root / "accepted"
    candidates_dir = root / "candidates"
    results_dir = root / "results"
    decisions_dir = root / "decisions"

    for directory in (
        root, iterations_dir, accepted_dir,
        candidates_dir, results_dir, decisions_dir
    ):
        directory.mkdir(parents=True, exist_ok=True)

    state_path = root / "rtl_verification_state.json"
    history_path = root / "rtl_repair_history.json"
    accepted_snapshot = accepted_dir / original_rtl_path.name

    def atomic_json(path, payload):
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True))
        os.replace(tmp, path)

    def load_state():
        if not state_path.exists():
            return {}
        try:
            data = json.loads(state_path.read_text())
        except Exception as exc:
            raise RuntimeError(
                f"Unable to read RTL verification state: {exc}"
            ) from exc
        return data if isinstance(data, dict) else {}

    def save_state(**updates):
        state = load_state()
        state.update(updates)
        atomic_json(state_path, state)
        return state

    def load_history():
        if not history_path.exists():
            return []
        try:
            data = json.loads(history_path.read_text())
        except Exception:
            return []
        return data if isinstance(data, list) else []

    def append_history(entry):
        history = load_history()
        history.append(entry)
        atomic_json(history_path, history)

    def initialize_accepted_rtl():
        """Create the initial accepted copy; never write to benchmark RTL."""
        if not original_rtl_path.exists():
            raise RuntimeError(f"RTL not found: {original_rtl_path}")
        tmp = accepted_dir / f".{accepted_snapshot.name}.initial.tmp"
        shutil.copy2(original_rtl_path, tmp)
        os.replace(tmp, accepted_snapshot)

    def promote_candidate(candidate_path):
        """Promote only a generated candidate to the accepted snapshot."""
        tmp = accepted_dir / f".{accepted_snapshot.name}.promote.tmp"
        shutil.copy2(candidate_path, tmp)
        os.replace(tmp, accepted_snapshot)

    def rel_to_workspace(path):
        return str(path.relative_to(HOST_WORKSPACE))

    # IMPORTANT: the benchmark RTL is immutable.  It is read once to initialize
    # the generated accepted snapshot.  Resume always uses that snapshot.
    state = load_state()
    if not accepted_snapshot.exists():
        initialize_accepted_rtl()
        state = save_state(
            accepted_iteration=0,
            next_iteration=1,
            status="ready",
            verified=False,
            accepted_rtl=str(accepted_snapshot),
            original_rtl=str(original_rtl_path),
            original_rtl_modified=False,
        )
    elif not original_rtl_path.exists():
        raise RuntimeError(f"Original RTL not found: {original_rtl_path}")

    if (
        state.get("status") == "verified" and state.get("verified") is True
    ) or (
        state.get("status") == "complete"
        and state.get("handoff_ready_to_next_stage") is True
        and state.get("rtl_outcome") in {"no_repair", "unresolved"}
    ):
        print("\n✓ RTL verification stage already completed for the accepted RTL snapshot.")
        print(f"  RTL outcome: {state.get('rtl_outcome', 'verified')}")
        print(f"  Accepted RTL: {accepted_snapshot}")
        print(f"  Original RTL preserved: {original_rtl_path}")
        if state.get("handoff_ready_to_next_stage"):
            print("✓ RTL stage is marked ready for the next pipeline stage (e.g. ORFS).")
        return

    for iteration in range(
        int(state.get("next_iteration", 1)),
        MAX_RTL_VERIFICATION_ITERATIONS + 1,
    ):
        iteration_dir = iterations_dir / f"iteration_{iteration:02d}"
        iteration_dir.mkdir(parents=True, exist_ok=True)

        # Baseline verification always uses the generated accepted snapshot.
        accepted_rtl_rel = rel_to_workspace(accepted_snapshot)
        result_rel = (
            f"{DESIGN_GENERATED_ROOT}/rtl_verification/results/"
            f"iteration_{iteration:02d}.json"
        )
        result_path = HOST_WORKSPACE / result_rel

        save_state(
            status="verification_in_progress",
            current_iteration=iteration,
            next_iteration=iteration,
            verified=False,
            accepted_rtl=str(accepted_snapshot),
            original_rtl=str(original_rtl_path),
            original_rtl_modified=False,
        )

        print(f"\n===== RTL VERIFICATION (iteration {iteration}) =====")
        print(f"  Accepted RTL snapshot: {accepted_snapshot}")
        print(f"  Benchmark RTL (read-only): {original_rtl_path}")

        get(
            simulate.chia_remote(
                accepted_rtl_rel,
                TB_DIR_REL,
                result_rel,
                SIM_TEST_TIMEOUT,
                SIM_BUILD_TIMEOUT,
            )
        )

        if not result_path.exists():
            raise RuntimeError(
                f"RTL verification result was not created: {result_path}"
            )

        shutil.copy2(result_path, iteration_dir / "simulation_result.json")
        analysis = analyze_results(str(result_path))
        # If this iteration's simulation never produced functional test
        # evidence (the TB failed to build/run at all), no LLM decision —
        # however schema-valid — is grounded enough to declare the RTL
        # correct. Force any such "no_repair" claim through the existing
        # non_actionable retry path instead of accepting it as completion.
        no_functional_evidence = analysis.get("detail", {}).get("status") in {
            "validation_error",
            "build_failure",
        }
        (iteration_dir / "verification_result.yaml").write_text(
            yaml.safe_dump(analysis, sort_keys=False)
        )

        # Deterministic RTL quality score for the currently accepted RTL.
        # This is evaluated with the frozen TB and is kept separate from the
        # RTL pass/fail verdict used by the existing repair/acceptance flow.
        baseline_score_report = analyze_verification_state(
            plan_path,
            tb_dir,
            result_path,
            iteration,
            None,
        )
        baseline_score_path = iteration_dir / "baseline_score.yaml"
        baseline_score_path.write_text(
            yaml.safe_dump(baseline_score_report, sort_keys=False)
        )

        print(json.dumps(analysis, indent=2))
        print(f"\n===== RTL BASELINE SCORE (iteration {iteration}) =====")
        print(
            f"Accepted RTL score: "
            f"{float(baseline_score_report.get('quality_score', 0.0)):.2f}"
        )
        print(
            "Accepted RTL metrics: "
            + json.dumps(baseline_score_report.get("metrics", {}), indent=2)
        )
        print(f"Baseline score report: {baseline_score_path}")

        if analysis.get("verdict") == "pass":
            save_state(
                status="verified",
                current_iteration=None,
                next_iteration=iteration + 1,
                verified=True,
                verified_iteration=iteration,
                accepted_rtl=str(accepted_snapshot),
                original_rtl=str(original_rtl_path),
                original_rtl_modified=False,
                verified_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
            )
            (root / "verification_result.yaml").write_text(
                yaml.safe_dump(
                    {
                        "status": "verified",
                        "iteration": iteration,
                        "result_path": str(result_path),
                        "accepted_rtl": str(accepted_snapshot),
                        "original_rtl": str(original_rtl_path),
                        "original_rtl_modified": False,
                        "tb": str(tb_dir),
                    },
                    sort_keys=False,
                )
            )
            print(
                "\n✓ RTL VERIFIED: all required tests passed with the frozen "
                "accepted TB."
            )
            print("✓ Original benchmark RTL was not modified.")
            return

        if analysis.get("verdict") == "template_bug":
            save_state(
                status="blocked",
                current_iteration=None,
                verified=False,
                stop_reason="template_bug",
                accepted_rtl=str(accepted_snapshot),
                original_rtl=str(original_rtl_path),
                original_rtl_modified=False,
            )
            raise RuntimeError(
                "RTL verification identified a shared toolchain/template defect; "
                "no RTL repair was attempted.\n\n"
                + json.dumps(analysis, indent=2)
            )

        # ---------------------------------------------------------
        # RTL failure analysis — only a COPY of the accepted RTL is exposed.
        # ---------------------------------------------------------
        analysis_workspace = root / f"llm_analysis_iteration_{iteration:02d}"
        if analysis_workspace.exists():
            shutil.rmtree(analysis_workspace)
        analysis_workspace.mkdir(parents=True, exist_ok=True)

        shutil.copy2(accepted_snapshot, analysis_workspace / "rtl.sv")
        shutil.copy2(HOST_WORKSPACE / SPEC, analysis_workspace / "specification.md")
        shutil.copy2(HOST_WORKSPACE / REF_MODEL, analysis_workspace / "reference_model.py")
        shutil.copy2(plan_path, analysis_workspace / "verification_plan.yaml")
        shutil.copytree(
            tb_dir,
            analysis_workspace / "tb",
            ignore=shutil.ignore_patterns(".chia_sim", "__pycache__", "*.so"),
        )
        shutil.copy2(result_path, analysis_workspace / "simulation_result.json")

        prior_iterations_context = format_prior_iterations_context(
            load_history(), iteration
        )

        analysis_llm, analysis_bash = create_improvement_agent(
            str(analysis_workspace),
            retries=LLM_RETRIES,
        )
        try:
            response = llm_get(
                analysis_llm.prompt.chia_remote(
                    analysis_llm,
                    build_rtl_failure_analysis_prompt(
                        analysis_workspace,
                        prior_iterations_context=prior_iterations_context,
                    ),
                    tools=[analysis_bash],
                )
            )
        finally:
            analysis_bash.stop()

        if not response.success:
            _raise_on_llm_failure(response, "RTL failure-analysis")

        decision_path = decisions_dir / f"iteration_{iteration:02d}.yaml"
        decision_text = ensure_valid_rtl_repair_decision_yaml(
            analysis_llm,
            response.result or "",
            max_attempts=5,
        )
        decision_path.write_text(decision_text)
        decision = yaml.safe_load(decision_text)

        valid, validation = validate_rtl_repair_decision(decision)
        if not valid:
            raise RuntimeError(
                f"Invalid RTL repair decision after YAML repair: {validation}"
            )

        (iteration_dir / "rtl_repair_decision.yaml").write_text(
            yaml.safe_dump(decision, sort_keys=False)
        )
        print("\n===== RTL REPAIR DECISION =====")
        print(yaml.safe_dump(decision, sort_keys=False))

        action = decision["action"]

        if action == "no_repair" and no_functional_evidence:
            print(
                "\n⚠ RTL repair decision was 'no_repair', but this "
                "iteration's simulation never produced functional test "
                f"evidence (status={analysis.get('detail', {}).get('status')!r}) "
                "— treating as non_actionable instead of accepting "
                "completion."
            )
            action = "non_actionable"
            decision["action"] = "non_actionable"

        if action == "no_repair":
            append_history(
                {
                    "iteration": iteration,
                    "result": "no_repair",
                    "root_cause": decision["root_cause"],
                    "confidence": decision["confidence"],
                    "decision_path": str(decision_path),
                    "accepted_rtl": str(accepted_snapshot),
                    "original_rtl": str(original_rtl_path),
                    "original_rtl_modified": False,
                }
            )
            save_state(
                status="complete",
                current_iteration=None,
                next_iteration=iteration + 1,
                verified=False,
                stop_reason="no_repair",
                rtl_outcome="no_repair",
                handoff_ready_to_next_stage=True,
                accepted_rtl=str(accepted_snapshot),
                original_rtl=str(original_rtl_path),
                original_rtl_modified=False,
                completed_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
            )
            (root / "verification_result.yaml").write_text(
                yaml.safe_dump(
                    {
                        "status": "rtl_stage_complete",
                        "rtl_outcome": "no_repair",
                        "reason": "no_repair",
                        "handoff_ready_to_next_stage": True,
                        "iteration": iteration,
                        "accepted_rtl": str(accepted_snapshot),
                        "original_rtl": str(original_rtl_path),
                        "original_rtl_modified": False,
                    },
                    sort_keys=False,
                )
            )
            mark_stage_done(
                "rtl_verification_complete",
                detail={
                    "iteration": iteration,
                    "rtl_outcome": "no_repair",
                    "handoff_ready_to_next_stage": True,
                    "accepted_rtl": str(accepted_snapshot),
                },
            )
            print("\n✓ RTL repair phase complete: no_repair.")
            print("  No RTL modification is warranted by the grounded diagnosis.")
            print("✓ RTL stage is ready for the next pipeline stage (e.g. ORFS).")
            print("✓ Original benchmark RTL was not modified.")
            return

        if action == "non_actionable":
            # Retry diagnosis inside the SAME RTL iteration.  This deliberately
            # does not advance `iteration`, does not run a candidate simulation,
            # and does not invoke any RTL plateau detector.
            non_actionable_retry_count = 0
            retry_context = (
                "Previous RTL repair decision:\n"
                + yaml.safe_dump(decision, sort_keys=False, allow_unicode=True)
                + "\n\n"
                "Previous simulation evidence:\n"
                + json.dumps(analysis, indent=2)
            )

            append_history(
                {
                    "iteration": iteration,
                    "result": "non_actionable",
                    "retry_count": non_actionable_retry_count,
                    "root_cause": decision["root_cause"],
                    "evidence": decision.get("evidence", []),
                    "confidence": decision["confidence"],
                    "decision_path": str(decision_path),
                    "accepted_rtl": str(accepted_snapshot),
                    "original_rtl": str(original_rtl_path),
                    "original_rtl_modified": False,
                }
            )

            retry_succeeded = False
            while non_actionable_retry_count < MAX_RTL_NON_ACTIONABLE_RETRIES:
                non_actionable_retry_count += 1
                retry_workspace = (
                    root
                    / f"llm_analysis_iteration_{iteration:02d}_retry_"
                    f"{non_actionable_retry_count:02d}"
                )
                if retry_workspace.exists():
                    shutil.rmtree(retry_workspace)
                retry_workspace.mkdir(parents=True, exist_ok=True)

                shutil.copy2(accepted_snapshot, retry_workspace / "rtl.sv")
                shutil.copy2(
                    HOST_WORKSPACE / SPEC,
                    retry_workspace / "specification.md",
                )
                shutil.copy2(
                    HOST_WORKSPACE / REF_MODEL,
                    retry_workspace / "reference_model.py",
                )
                shutil.copy2(
                    plan_path,
                    retry_workspace / "verification_plan.yaml",
                )
                shutil.copytree(
                    tb_dir,
                    retry_workspace / "tb",
                    ignore=shutil.ignore_patterns(
                        ".chia_sim", "__pycache__", "*.so"
                    ),
                )
                shutil.copy2(
                    result_path,
                    retry_workspace / "simulation_result.json",
                )

                print(
                    f"\n===== RTL NON-ACTIONABLE DIAGNOSIS RETRY "
                    f"{non_actionable_retry_count}/"
                    f"{MAX_RTL_NON_ACTIONABLE_RETRIES} "
                    f"(iteration {iteration}) ====="
                )

                retry_llm, retry_bash = create_improvement_agent(
                    str(retry_workspace),
                    retries=LLM_RETRIES,
                )
                try:
                    retry_response = llm_get(
                        retry_llm.prompt.chia_remote(
                            retry_llm,
                            build_rtl_failure_analysis_prompt(
                                retry_workspace,
                                retry_context=retry_context,
                                prior_iterations_context=prior_iterations_context,
                            ),
                            tools=[retry_bash],
                        )
                    )
                finally:
                    retry_bash.stop()

                if not retry_response.success:
                    _raise_on_llm_failure(retry_response, "RTL non-actionable retry")

                retry_decision_path = (
                    decisions_dir
                    / f"iteration_{iteration:02d}_retry_"
                    f"{non_actionable_retry_count:02d}.yaml"
                )
                retry_decision_text = ensure_valid_rtl_repair_decision_yaml(
                    retry_llm,
                    retry_response.result or "",
                    max_attempts=5,
                )
                retry_decision_path.write_text(retry_decision_text)
                retry_decision = yaml.safe_load(retry_decision_text)

                valid, validation = validate_rtl_repair_decision(retry_decision)
                if not valid:
                    raise RuntimeError(
                        "Invalid RTL repair decision after non-actionable "
                        f"retry YAML repair: {validation}"
                    )

                (retry_workspace / "rtl_repair_decision.yaml").write_text(
                    yaml.safe_dump(retry_decision, sort_keys=False)
                )
                (iteration_dir / f"rtl_repair_decision_retry_"
                 f"{non_actionable_retry_count:02d}.yaml").write_text(
                    yaml.safe_dump(retry_decision, sort_keys=False)
                )

                append_history(
                    {
                        "iteration": iteration,
                        "result": retry_decision["action"],
                        "retry_count": non_actionable_retry_count,
                        "root_cause": retry_decision["root_cause"],
                        "evidence": retry_decision.get("evidence", []),
                        "confidence": retry_decision["confidence"],
                        "decision_path": str(retry_decision_path),
                        "accepted_rtl": str(accepted_snapshot),
                        "original_rtl": str(original_rtl_path),
                        "original_rtl_modified": False,
                    }
                )

                action = retry_decision["action"]
                decision = retry_decision
                decision_path = retry_decision_path

                if action == "no_repair" and no_functional_evidence:
                    print(
                        "\n⚠ RTL repair retry decision was 'no_repair', but "
                        "this iteration's simulation never produced "
                        "functional test evidence (status="
                        f"{analysis.get('detail', {}).get('status')!r}) — "
                        "treating as non_actionable instead of accepting "
                        "completion."
                    )
                    action = "non_actionable"
                    decision["action"] = "non_actionable"

                if action != "non_actionable":
                    retry_succeeded = True
                    break

                retry_context = (
                    "Previous RTL repair decision (non_actionable):\n"
                    + yaml.safe_dump(
                        retry_decision,
                        sort_keys=False,
                        allow_unicode=True,
                    )
                    + "\n\n"
                    "Previous simulation evidence:\n"
                    + json.dumps(analysis, indent=2)
                )

            if not retry_succeeded:
                save_state(
                    status="complete",
                    current_iteration=None,
                    next_iteration=iteration + 1,
                    verified=False,
                    stop_reason="non_actionable",
                    rtl_outcome="unresolved",
                    handoff_ready_to_next_stage=True,
                    non_actionable_retries=non_actionable_retry_count,
                    accepted_rtl=str(accepted_snapshot),
                    original_rtl=str(original_rtl_path),
                    original_rtl_modified=False,
                    completed_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
                )
                (root / "verification_result.yaml").write_text(
                    yaml.safe_dump(
                        {
                            "status": "rtl_stage_complete",
                            "rtl_outcome": "unresolved",
                            "reason": "non_actionable",
                            "handoff_ready_to_next_stage": True,
                            "iteration": iteration,
                            "non_actionable_retries": non_actionable_retry_count,
                            "accepted_rtl": str(accepted_snapshot),
                            "original_rtl": str(original_rtl_path),
                            "original_rtl_modified": False,
                        },
                        sort_keys=False,
                    )
                )
                mark_stage_done(
                    "rtl_verification_complete",
                    detail={
                        "iteration": iteration,
                        "rtl_outcome": "unresolved",
                        "stop_reason": "non_actionable",
                        "non_actionable_retries": non_actionable_retry_count,
                        "handoff_ready_to_next_stage": True,
                        "accepted_rtl": str(accepted_snapshot),
                    },
                )
                print(
                    "\n✓ RTL repair phase complete: diagnosis remained "
                    "non_actionable after "
                    f"{non_actionable_retry_count} retries."
                )
                print("  RTL outcome is recorded as unresolved; no speculative RTL repair was made.")
                print("✓ RTL stage is ready for the next pipeline stage (e.g. ORFS), subject to that stage's input requirements.")
                print("✓ Original benchmark RTL was not modified.")
                return

            # A retry produced a new actionable decision. Continue through the
            # normal decision handling below without consuming another RTL
            # verification iteration.
            if action == "no_repair":
                save_state(
                    status="complete",
                    current_iteration=None,
                    next_iteration=iteration + 1,
                    verified=False,
                    stop_reason="no_repair",
                    rtl_outcome="no_repair",
                    handoff_ready_to_next_stage=True,
                    non_actionable_retries=non_actionable_retry_count,
                    accepted_rtl=str(accepted_snapshot),
                    original_rtl=str(original_rtl_path),
                    original_rtl_modified=False,
                    completed_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
                )
                (root / "verification_result.yaml").write_text(
                    yaml.safe_dump(
                        {
                            "status": "rtl_stage_complete",
                            "rtl_outcome": "no_repair",
                            "reason": "no_repair",
                            "handoff_ready_to_next_stage": True,
                            "iteration": iteration,
                            "non_actionable_retries": non_actionable_retry_count,
                            "accepted_rtl": str(accepted_snapshot),
                            "original_rtl": str(original_rtl_path),
                            "original_rtl_modified": False,
                        },
                        sort_keys=False,
                    )
                )
                mark_stage_done(
                    "rtl_verification_complete",
                    detail={
                        "iteration": iteration,
                        "rtl_outcome": "no_repair",
                        "handoff_ready_to_next_stage": True,
                        "non_actionable_retries": non_actionable_retry_count,
                        "accepted_rtl": str(accepted_snapshot),
                    },
                )
                print("\n✓ RTL repair phase complete: retry diagnosis returned no_repair.")
                print("  No RTL modification is warranted by the grounded diagnosis.")
                print("✓ RTL stage is ready for the next pipeline stage (e.g. ORFS).")
                print("✓ Original benchmark RTL was not modified.")
                return

            if action == "template_bug":
                save_state(
                    status="blocked",
                    current_iteration=None,
                    next_iteration=iteration + 1,
                    verified=False,
                    stop_reason="template_bug",
                    non_actionable_retries=non_actionable_retry_count,
                    accepted_rtl=str(accepted_snapshot),
                    original_rtl=str(original_rtl_path),
                    original_rtl_modified=False,
                )
                raise RuntimeError(
                    "RTL repair analysis classified the failure as a "
                    "template/toolchain bug after non-actionable retry."
                )

        if action == "template_bug":
            append_history(
                {
                    "iteration": iteration,
                    "result": "template_bug",
                    "root_cause": decision["root_cause"],
                    "decision_path": str(decision_path),
                    "accepted_rtl": str(accepted_snapshot),
                    "original_rtl": str(original_rtl_path),
                    "original_rtl_modified": False,
                }
            )
            save_state(
                status="blocked",
                current_iteration=None,
                next_iteration=iteration + 1,
                verified=False,
                stop_reason="template_bug",
                accepted_rtl=str(accepted_snapshot),
                original_rtl=str(original_rtl_path),
                original_rtl_modified=False,
            )
            raise RuntimeError(
                "RTL repair analysis classified the failure as a "
                "template/toolchain bug."
            )

        # ---------------------------------------------------------
        # Grounding check — before spending a repair-apply LLM call and a
        # full simulation, verify the decision's cited "old" RTL text
        # actually exists in the current accepted RTL. A decision built on
        # a hallucinated read of the file (e.g. citing a line's content
        # that isn't really there) cannot produce a meaningful repair, and
        # letting it through wastes an entire apply+simulate cycle to
        # discover that. Conservative by design: only flags instructions
        # matching the "replace/change X with/to Y" phrasing this pipeline
        # consistently uses; anything else is left to the normal
        # apply+simulate+reject path.
        # ---------------------------------------------------------
        ungrounded = find_ungrounded_rtl_changes(
            decision, accepted_snapshot.read_text()
        )
        if ungrounded:
            print("\n✗ RTL repair decision is not grounded in the current RTL:")
            for problem in ungrounded:
                print(f"  - {problem}")
            append_history(
                {
                    "iteration": iteration,
                    "result": "ungrounded_reference",
                    "root_cause": decision["root_cause"],
                    "confidence": decision["confidence"],
                    "decision_path": str(decision_path),
                    "ungrounded_problems": ungrounded,
                    "accepted_rtl": str(accepted_snapshot),
                    "original_rtl": str(original_rtl_path),
                    "original_rtl_modified": False,
                }
            )
            save_state(
                status="ready",
                current_iteration=None,
                next_iteration=iteration + 1,
                verified=False,
                stop_reason="ungrounded_reference",
                accepted_rtl=str(accepted_snapshot),
                original_rtl=str(original_rtl_path),
                original_rtl_modified=False,
            )
            continue

        # ---------------------------------------------------------
        # RTL repair candidate — only the isolated copy may be edited.
        # ---------------------------------------------------------
        repair_workspace = root / f"llm_repair_iteration_{iteration:02d}"
        if repair_workspace.exists():
            shutil.rmtree(repair_workspace)
        repair_workspace.mkdir(parents=True, exist_ok=True)

        shutil.copy2(accepted_snapshot, repair_workspace / "rtl.sv")
        shutil.copy2(decision_path, repair_workspace / "rtl_repair_decision.yaml")
        shutil.copy2(result_path, repair_workspace / "simulation_result.json")
        shutil.copy2(HOST_WORKSPACE / SPEC, repair_workspace / "specification.md")
        shutil.copy2(HOST_WORKSPACE / REF_MODEL, repair_workspace / "reference_model.py")
        shutil.copy2(plan_path, repair_workspace / "verification_plan.yaml")
        shutil.copytree(
            tb_dir,
            repair_workspace / "tb",
            ignore=shutil.ignore_patterns(".chia_sim", "__pycache__", "*.so"),
        )

        repair_llm, repair_bash = create_improvement_agent(
            str(repair_workspace),
            retries=LLM_RETRIES,
        )
        try:
            repair_response = llm_get(
                repair_llm.prompt.chia_remote(
                    repair_llm,
                    build_rtl_repair_prompt(repair_workspace),
                    tools=[repair_bash],
                )
            )
        finally:
            repair_bash.stop()

        if not repair_response.success:
            _raise_on_llm_failure(repair_response, "RTL repair")

        candidate_rtl = repair_workspace / "rtl.sv"
        if not candidate_rtl.exists():
            raise RuntimeError("RTL repair LLM did not produce rtl.sv.")

        # Compare against the accepted snapshot, never against benchmark RTL.
        if candidate_rtl.read_bytes() == accepted_snapshot.read_bytes():
            append_history(
                {
                    "iteration": iteration,
                    "result": "no_change",
                    "root_cause": decision["root_cause"],
                    "decision_path": str(decision_path),
                    "accepted_rtl": str(accepted_snapshot),
                    "original_rtl": str(original_rtl_path),
                    "original_rtl_modified": False,
                }
            )
            save_state(
                status="ready",
                current_iteration=None,
                next_iteration=iteration + 1,
                verified=False,
                stop_reason="repair_produced_no_change",
                accepted_rtl=str(accepted_snapshot),
                original_rtl=str(original_rtl_path),
                original_rtl_modified=False,
            )
            continue

        # Store candidate under generated/rtl_verification/.  This is the ONLY
        # RTL path that the repair candidate simulation sees.
        candidate_dir = candidates_dir / f"iteration_{iteration:02d}"
        if candidate_dir.exists():
            shutil.rmtree(candidate_dir)
        candidate_dir.mkdir(parents=True, exist_ok=True)
        candidate_snapshot = candidate_dir / original_rtl_path.name
        shutil.copy2(candidate_rtl, candidate_snapshot)

        candidate_result_rel = (
            f"{DESIGN_GENERATED_ROOT}/rtl_verification/results/"
            f"candidate_{iteration:02d}.json"
        )
        candidate_result_path = HOST_WORKSPACE / candidate_result_rel

        print("\n===== RTL CANDIDATE VERIFICATION =====")
        print(f"  Candidate RTL: {candidate_snapshot}")
        print(f"  Benchmark RTL remains untouched: {original_rtl_path}")

        get(
            simulate.chia_remote(
                rel_to_workspace(candidate_snapshot),
                TB_DIR_REL,
                candidate_result_rel,
                SIM_TEST_TIMEOUT,
                SIM_BUILD_TIMEOUT,
            )
        )

        if not candidate_result_path.exists():
            raise RuntimeError(
                f"RTL candidate result was not created: {candidate_result_path}"
            )

        shutil.copy2(
            candidate_result_path,
            iteration_dir / "candidate_simulation_result.json",
        )
        candidate_analysis = analyze_results(str(candidate_result_path))
        (iteration_dir / "candidate_verification_result.yaml").write_text(
            yaml.safe_dump(candidate_analysis, sort_keys=False)
        )

        # Score the RTL candidate with exactly the same frozen TB and
        # deterministic scorer used for the accepted RTL baseline.  A passing
        # candidate is always accepted.  A still-failing candidate may also
        # become the new accepted baseline when it improves the deterministic
        # quality score enough; this is what enables multi-iteration RTL repair.
        candidate_score_report = analyze_verification_state(
            plan_path,
            tb_dir,
            candidate_result_path,
            iteration,
            baseline_score_report,
        )
        candidate_score_path = iteration_dir / "candidate_score.yaml"
        candidate_score_path.write_text(
            yaml.safe_dump(candidate_score_report, sort_keys=False)
        )

        baseline_score = float(
            baseline_score_report.get("quality_score", 0.0)
        )
        candidate_score = float(
            candidate_score_report.get("quality_score", 0.0)
        )
        baseline_failed = int(
            baseline_score_report.get("metrics", {}).get("tests_failed", 0)
        )
        candidate_failed = int(
            candidate_score_report.get("metrics", {}).get("tests_failed", 0)
        )

        print(f"\n===== RTL CANDIDATE SCORE (iteration {iteration}) =====")
        print(
            f"Accepted RTL score: {baseline_score:.2f} | "
            f"Candidate RTL score: {candidate_score:.2f}"
        )
        print(
            f"Accepted failed: {baseline_failed} | "
            f"Candidate failed: {candidate_failed}"
        )
        print(
            "Candidate RTL metrics: "
            + json.dumps(candidate_score_report.get("metrics", {}), indent=2)
        )
        print(f"Candidate score report: {candidate_score_path}")

        candidate_passed = candidate_analysis.get("verdict") == "pass"
        candidate_built = _candidate_rtl_built(candidate_analysis)
        if not candidate_built:
            print(
                "Candidate RTL failed to build (every test exited with "
                "returncode 2) -- rejecting despite an equal quality score, "
                "which only ties because no test actually ran."
            )
        non_regression = candidate_built and candidate_score >= baseline_score

        if candidate_passed or non_regression:
            # Promotion occurs only between generated snapshots.
            promote_candidate(candidate_snapshot)

            if candidate_passed:
                acceptance_reason = "passed_all_required_tests"
                next_status = "verified"
            else:
                acceptance_reason = "no_regression"
                next_status = "ready"

            append_history(
                {
                    "iteration": iteration,
                    "result": "accepted",
                    "acceptance_reason": acceptance_reason,
                    "root_cause": decision["root_cause"],
                    "confidence": decision["confidence"],
                    "decision_path": str(decision_path),
                    "candidate_result": str(candidate_result_path),
                    "baseline_quality_score": baseline_score,
                    "candidate_quality_score": candidate_score,
                    "score_delta": candidate_score - baseline_score,
                    "baseline_tests_failed": baseline_failed,
                    "candidate_tests_failed": candidate_failed,
                    "accepted_rtl": str(accepted_snapshot),
                    "original_rtl": str(original_rtl_path),
                    "original_rtl_modified": False,
                }
            )

            if candidate_passed:
                save_state(
                    status="verified",
                    current_iteration=None,
                    next_iteration=iteration + 1,
                    verified=True,
                    verified_iteration=iteration,
                    accepted_iteration=iteration,
                    accepted_rtl=str(accepted_snapshot),
                    original_rtl=str(original_rtl_path),
                    original_rtl_modified=False,
                    verified_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
                )
                (root / "verification_result.yaml").write_text(
                    yaml.safe_dump(
                        {
                            "status": "verified",
                            "iteration": iteration,
                            "result_path": str(candidate_result_path),
                            "accepted_rtl": str(accepted_snapshot),
                            "original_rtl": str(original_rtl_path),
                            "original_rtl_modified": False,
                            "tb": str(tb_dir),
                        },
                        sort_keys=False,
                    )
                )
                print(
                    f"\n✓ RTL repair accepted at iteration {iteration}; "
                    "all required tests passed and the generated RTL snapshot "
                    "is VERIFIED."
                )
                print("✓ Original benchmark RTL was not modified.")
                return

            # A failing but objectively improved candidate becomes the new
            # accepted baseline.  The next iteration therefore starts from
            # this candidate rather than reverting to the older RTL.
            save_state(
                status=next_status,
                current_iteration=None,
                next_iteration=iteration + 1,
                verified=False,
                accepted_iteration=iteration,
                last_result="accepted_improvement",
                accepted_rtl=str(accepted_snapshot),
                original_rtl=str(original_rtl_path),
                original_rtl_modified=False,
            )
            print(
                f"\n✓ RTL repair accepted (no regression) at "
                f"iteration {iteration}."
                f" Score: {baseline_score:.2f} -> {candidate_score:.2f} "
                f"(Δ={candidate_score - baseline_score:.2f})."
            )
            print("  Continuing from the updated accepted RTL on the next iteration.")
            print("✓ Original benchmark RTL was not modified.")
            continue

        # Rejected candidates are simply discarded.  The accepted snapshot
        # remains the only durable RTL state.
        append_history(
            {
                "iteration": iteration,
                "result": "rejected",
                "root_cause": decision["root_cause"],
                "confidence": decision["confidence"],
                "decision_path": str(decision_path),
                "candidate_result": str(candidate_result_path),
                "candidate_verdict": candidate_analysis.get("verdict"),
                "baseline_quality_score": baseline_score,
                "candidate_quality_score": candidate_score,
                "score_delta": candidate_score - baseline_score,
                "baseline_tests_failed": baseline_failed,
                "candidate_tests_failed": candidate_failed,
                "accepted_rtl": str(accepted_snapshot),
                "original_rtl": str(original_rtl_path),
                "original_rtl_modified": False,
            }
        )
        save_state(
            status="ready",
            current_iteration=None,
            next_iteration=iteration + 1,
            verified=False,
            last_result="rejected",
            accepted_rtl=str(accepted_snapshot),
            original_rtl=str(original_rtl_path),
            original_rtl_modified=False,
        )
        print(
            f"\n✗ RTL repair rejected at iteration {iteration}; "
            "candidate regressed the accepted RTL's quality score."
        )
        print(
            f"  Score: {baseline_score:.2f} -> {candidate_score:.2f} "
            f"(Δ={candidate_score - baseline_score:.2f})."
        )
        print("✓ Original benchmark RTL was not modified.")

    save_state(
        status="incomplete",
        current_iteration=None,
        next_iteration=MAX_RTL_VERIFICATION_ITERATIONS + 1,
        verified=False,
        stop_reason="max_iterations",
        accepted_rtl=str(accepted_snapshot),
        original_rtl=str(original_rtl_path),
        original_rtl_modified=False,
    )
    (root / "verification_result.yaml").write_text(
        yaml.safe_dump(
            {
                "status": "verification_incomplete",
                "reason": "max_iterations",
                "max_iterations": MAX_RTL_VERIFICATION_ITERATIONS,
                "accepted_rtl": str(accepted_snapshot),
                "original_rtl": str(original_rtl_path),
                "original_rtl_modified": False,
            },
            sort_keys=False,
        )
    )
    print(
        "\n⚠ RTL verification incomplete: maximum RTL verification "
        "iterations reached."
    )
    print("✓ Original benchmark RTL was not modified.")


# Main ARGUS orchestration

# =========================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="ARGUS multi-RTL Cocotb + pyUVM verification pipeline"
    )
    parser.add_argument(
        "--design-config",
        default="benchmarks/fifo/design.yaml",
        help="Project-relative YAML benchmark configuration",
    )
    args = parser.parse_args()

    configure_design(args.design_config)

    print("\n=========================================================")
    print(f" ARGUS verification run: {DESIGN_NAME}")
    print("=========================================================")
    print(f"  RTL:          {RTL}")
    print(f"  Specification:{SPEC}")
    print(f"  Reference:    {REF_MODEL}")
    print(f"  Generated TB: {TB_DIR_REL}")
    print(f"  Plan:         {PLAN}")

    ray.init()

    try:

        plan_path = HOST_WORKSPACE / PLAN
        rtl_info_path = HOST_WORKSPACE / RTL_INFO
        candidate_path = HOST_WORKSPACE / CANDIDATE_PLAN

        # =================================================
        # Fast path: verification plan + rtl_info already exist.
        #
        # Skip RTL extraction, plan generation, and plan review
        # entirely and go straight to UVM environment generation
        # (Stage 4) and its closed-loop validation (Stage 5), which
        # in turn skip any of their own sub-stages that already have
        # a checkpoint from a previous run.
        # =================================================
        if plan_path.exists() and rtl_info_path.exists():

            print(
                "\n✓ Found existing verification plan and rtl_info.json "
                "— skipping extraction, generation, and review."
            )
            print(f"  Plan:     {plan_path}")
            print(f"  RTL info: {rtl_info_path}")

            canonical_plan = ensure_directed_scenario_ids(plan_path)

            # If Stage 4 already produced a complete TB, it is persistent
            # state and must never be regenerated just because run8.py was
            # invoked again. generate_and_validate_uvm() performs the same
            # guard as a second line of defense.
            if has_existing_generated_tb() and not FORCE_SCENARIO_MANIFEST_REGEN:
                print(
                    "\n✓ Existing generated_tb detected — resuming from "
                    "Stage 5+ without regenerating the environment."
                )

                # No Stage-4 generation is needed. Avoid allocating the
                # single OpenCode tool slot; Stage 5/6..9 create their own
                # sanitized agents as required.
                generate_and_validate_uvm(None, None, canonical_plan)
            else:
                llm, bash = create_agent()
                try:
                    generate_and_validate_uvm(llm, bash, canonical_plan)
                finally:
                    # Normally Stage 4 hands the resource back before
                    # Stage 5. stop() is retained here as the final safety
                    # net for exceptions during generation.
                    bash.stop()

            return

        # =================================================
        # No accepted plan yet. Resume as far into stages 1-3 as
        # existing artifacts allow, then fall through into stage 4/5
        # once a plan is accepted.
        # =================================================

        llm, bash = create_agent()

        try:

            # -------------------------------------------------
            # Stage 1: RTL extraction
            # -------------------------------------------------
            if rtl_info_path.exists():
                print(
                    "\n✓ Found existing rtl_info.json — skipping RTL "
                    f"extraction.\n  RTL info: {rtl_info_path}"
                )
            else:
                print("\n[1/4] Extracting RTL information...")

                rtl_info = get(

                    extract_rtl.chia_remote(

                        RTL,

                        RTL_INFO,

                    )

                )

                print(f"RTL information: {rtl_info}")

            # -------------------------------------------------
            # Stage 2: Generate (or resume) candidate plan
            #
            # ensure_valid_yaml() only retries *syntax* repair (asking the
            # LLM to fix its own malformed YAML). That's the wrong tool for
            # a structural failure like "missing required top-level field:
            # ports" or "Plan must be a YAML mapping" -- repeatedly asking
            # to "fix the syntax" of content that is structurally wrong
            # predictably keeps failing the same way, and ensure_valid_yaml
            # hard-raises after 5 such attempts with nothing here to catch
            # it, crashing the whole process. Wrap both the resume and the
            # fresh-generation paths in an outer loop that, on that
            # RuntimeError, discards the bad candidate and asks for an
            # entirely fresh one via generate_candidate_plan() instead of
            # retrying syntax-repair on content that was never going to
            # syntax-repair its way to valid.
            # -------------------------------------------------
            MAX_CANDIDATE_REGEN_ATTEMPTS = 3
            candidate = None
            for regen_attempt in range(1, MAX_CANDIDATE_REGEN_ATTEMPTS + 1):
                try:
                    if candidate_path.exists():
                        print(
                            "\n✓ Found existing candidate verification plan — "
                            f"skipping generation and resuming at review.\n"
                            f"  Candidate: {candidate_path}"
                        )
                        candidate = ensure_valid_yaml(
                            llm,
                            candidate_path.read_text(),
                            "resumed candidate",
                            max_attempts=5,
                        )
                        candidate_path.write_text(candidate)
                    else:
                        print(
                            f"\n[2/4] Generating verification plan "
                            f"(attempt {regen_attempt}/{MAX_CANDIDATE_REGEN_ATTEMPTS})..."
                        )
                        raw_candidate = generate_candidate_plan(llm, bash)
                        candidate = ensure_valid_yaml(
                            llm,
                            raw_candidate,
                            "candidate generation",
                            max_attempts=5,
                        )
                        print("\n✓ Candidate verification plan generated.")
                        candidate_path.parent.mkdir(parents=True, exist_ok=True)
                        candidate = clean_yaml_response(candidate)
                        candidate_path.write_text(candidate)
                        print(f"✓ Candidate saved to: {candidate_path}")
                    break
                except RuntimeError as exc:
                    if _is_model_unavailable(exc):
                        raise   # provider is down; cool it down and let the supervisor switch
                    if regen_attempt >= MAX_CANDIDATE_REGEN_ATTEMPTS:
                        raise
                    print(
                        f"\n⚠ Candidate plan structurally invalid on attempt "
                        f"{regen_attempt}/{MAX_CANDIDATE_REGEN_ATTEMPTS} "
                        f"(syntax-repair exhausted): {exc}\n"
                        "  Discarding it and generating a fresh candidate "
                        "from scratch instead of crashing."
                    )
                    if candidate_path.exists():
                        candidate_path.unlink()

            # =================================================

            # Stage 3

            # Independent LLM review

            # =================================================

            repair_yaml_error = None
            plan_review_history_path = candidate_path.parent / "plan_review_history.json"

            for attempt in range(MAX_REPAIR_ATTEMPTS + 1):

                print(
                    f"\n[3/4] Reviewing verification plan "
                    f"(attempt {attempt + 1})..."
                )

                review = review_candidate_plan(
                    llm,
                    bash,
                    candidate,
                )

                if not review or not review.strip():
                    raise RuntimeError(
                        "Reviewer produced no usable response."
                    )

                print("\n===== REVIEW =====")
                print(review)

                status = parse_review_status(review)

                if status == "no_issues":

                    cleaned_candidate = ensure_valid_yaml(
                        llm,
                        candidate,
                        "final candidate",
                        max_attempts=5,
                    )

                    valid, parsed = validate_yaml(cleaned_candidate)

                    if not valid:
                        raise RuntimeError(
                            "Reviewer accepted the plan, but the "
                            f"candidate failed YAML validation:\n{parsed}"
                        )

                    # The LLM YAML is only an intermediate representation.
                    # Validate it against the canonical VerificationPlan
                    # before promoting it to the downstream artifact.
                    schema_validation_error = None
                    try:
                        accepted_plan = VerificationPlan.model_validate(parsed)
                    except Exception as exc:
                        # A repair-loop edit can leave a content-free stub
                        # entry behind (observed live: a directed_test_scenarios
                        # entry that is only `{name: null}`, no description,
                        # stimulus, or expected -- no scenario content to
                        # salvage, so drop it rather than invent one). Retry
                        # validation once; this is idempotent (a plan with no
                        # such stubs is returned unchanged) and re-raises the
                        # original error if that wasn't the actual problem.
                        pruned, dropped = _drop_empty_scenario_stubs(parsed)
                        if dropped:
                            print(
                                f"\n⚠ Dropping {len(dropped)} content-free scenario "
                                f"stub(s) from the accepted plan: {dropped}"
                            )
                            try:
                                accepted_plan = VerificationPlan.model_validate(pruned)
                                parsed = pruned
                            except Exception as exc2:
                                schema_validation_error = (
                                    "Reviewer accepted the plan, but it failed "
                                    "canonical VerificationPlan validation even "
                                    f"after dropping empty stubs {dropped}:\n{exc2}"
                                )
                        else:
                            schema_validation_error = (
                                "Reviewer accepted the plan, but it failed "
                                "canonical VerificationPlan validation:\n"
                                f"{exc}"
                            )

                    if schema_validation_error is not None:
                        # The reviewer only checks semantics, not schema
                        # shape -- it approved a plan the LLM generator got
                        # wrong in a way stub-dropping can't fix (e.g. a
                        # bool where a string is required, an int where a
                        # list is required). Previously this raised
                        # immediately and crashed run14 outright: since the
                        # on-disk candidate is unchanged, a supervisor
                        # restart resumes review on the SAME candidate, the
                        # reviewer is very likely to say "no_issues" again,
                        # and validation fails identically -- an infinite
                        # crash-loop. Feed the validation error back into
                        # the same repair budget/history used for semantic
                        # rejections instead, so the LLM coerces the types
                        # and the loop keeps making progress.
                        print(
                            f"\n✗ Verification plan rejected (schema "
                            f"validation): {schema_validation_error}"
                        )

                        plan_review_history = _append_json_list_history(
                            plan_review_history_path,
                            {"attempt": attempt + 1, "review": schema_validation_error},
                        )

                        if attempt >= MAX_REPAIR_ATTEMPTS:
                            raise RuntimeError(
                                "\nVerification plan repeatedly passed semantic "
                                "review but failed canonical VerificationPlan "
                                "schema validation.\n\n"
                                "The candidate has NOT been promoted to "
                                "verification_plan.yaml.\n\n"
                                f"Candidate remains available at:\n"
                                f"{candidate_path}\n\n"
                                f"Final validation error:\n{schema_validation_error}"
                            )

                        print("\nRepairing candidate plan for schema validation...")
                        print(
                            f"Repair attempt {attempt + 1}/"
                            f"{MAX_REPAIR_ATTEMPTS}..."
                        )

                        prior_reviews_context = format_prior_plan_reviews_context(
                            plan_review_history, attempt + 1
                        )

                        raw_repaired_candidate = repair_candidate_plan(
                            llm,
                            bash,
                            candidate,
                            (
                                "The plan passed semantic review but failed "
                                "strict schema validation against the "
                                "canonical VerificationPlan model. Fix ONLY "
                                "the type/shape mismatches identified below; "
                                "do not otherwise change the plan's content "
                                "or meaning:\n\n" + schema_validation_error
                            ),
                            repair_yaml_error,
                            prior_reviews_context=prior_reviews_context,
                        )

                        try:
                            candidate = ensure_valid_yaml(
                                llm,
                                raw_repaired_candidate,
                                "schema repair",
                                max_attempts=5,
                            )
                        except RuntimeError as exc:
                            if _is_model_unavailable(exc):
                                raise   # provider is down, not bad output
                            # Syntax-repair exhausted its own 5 attempts on
                            # this repair output. Don't let that crash the
                            # whole process: fold it into this loop's own
                            # attempt budget instead, same as an ordinary
                            # rejected/failed repair. `candidate` still
                            # holds the last known-good YAML (never
                            # overwritten on failure), so the next attempt
                            # retries repair from a valid starting point.
                            print(
                                f"\n⚠ Schema-repair YAML unusable after "
                                f"syntax-repair exhausted: {exc}\n"
                                "  Retrying repair from the last valid "
                                "candidate instead of crashing."
                            )
                            repair_yaml_error = str(exc)
                            continue
                        repair_yaml_error = None
                        candidate_path.write_text(candidate)
                        print(f"Updated candidate saved to: {candidate_path}")
                        continue

                    # Serialize the validated model, not the raw LLM YAML.
                    canonical_plan = yaml.safe_dump(
                        accepted_plan.model_dump(mode="json"),
                        sort_keys=False,
                        allow_unicode=True,
                    )

                    plan_path.parent.mkdir(
                        parents=True,
                        exist_ok=True,
                    )

                    plan_path.write_text(canonical_plan)
                    canonical_plan = ensure_directed_scenario_ids(plan_path)

                    print("\n✓ Verification plan accepted.")
                    print(f"✓ Final canonical plan saved to: {plan_path}")

                    # =================================================
                    # Stage 4 + 5: Generate UVM environment directly
                    # from the accepted plan, then validate it. Any
                    # sub-stage already checkpointed from a previous
                    # run is skipped automatically.
                    # =================================================

                    generate_and_validate_uvm(llm, bash, canonical_plan)

                    return

                print("\n✗ Verification plan rejected.")

                plan_review_history = _append_json_list_history(
                    plan_review_history_path,
                    {"attempt": attempt + 1, "review": review},
                )

                if attempt >= MAX_REPAIR_ATTEMPTS:

                    raise RuntimeError(
                        "\nVerification plan failed independent "
                        "semantic review.\n\n"
                        "The candidate has NOT been promoted to "
                        "verification_plan.yaml.\n\n"
                        f"Candidate remains available at:\n"
                        f"{candidate_path}\n\n"
                        f"Final review:\n{review}"
                    )

                print("\nRepairing candidate plan...")
                print(
                    f"Repair attempt {attempt + 1}/"
                    f"{MAX_REPAIR_ATTEMPTS}..."
                )

                prior_reviews_context = format_prior_plan_reviews_context(
                    plan_review_history, attempt + 1
                )

                raw_repaired_candidate = repair_candidate_plan(
                    llm,
                    bash,
                    candidate,
                    review,
                    repair_yaml_error,
                    prior_reviews_context=prior_reviews_context,
                )

                try:
                    candidate = ensure_valid_yaml(
                        llm,
                        raw_repaired_candidate,
                        "semantic repair",
                        max_attempts=5,
                    )
                except RuntimeError as exc:
                    if _is_model_unavailable(exc):
                        raise   # provider is down, not bad output
                    # Same crash-loop shape as the schema-repair site above:
                    # don't let syntax-repair exhaustion on this repair
                    # attempt's output crash the whole process. Fold it
                    # into this loop's own attempt budget instead.
                    # `candidate` still holds the last known-good YAML.
                    print(
                        f"\n⚠ Semantic-repair YAML unusable after "
                        f"syntax-repair exhausted: {exc}\n"
                        "  Retrying repair from the last valid candidate "
                        "instead of crashing."
                    )
                    repair_yaml_error = str(exc)
                    continue

                repair_yaml_error = None

                candidate_path.write_text(candidate)

                print(
                    f"Updated candidate saved to: "
                    f"{candidate_path}"
                )

        finally:

            bash.stop()

    finally:

        ray.shutdown()



if __name__ == "__main__":

    try:
        main()
    except BaseException as exc:
        _record_llm_rate_limit(exc)
        raise