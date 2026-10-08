#!/usr/bin/env python3
"""Run a generated ARGUS Cocotb + pyUVM environment with Verilator.

The worker deliberately keeps execution deterministic:
  * generated_tb/generation_manifest.yaml is the source of TB file order;
  * the Python reference model is used directly by the generated pyUVM TB;
  * Cocotb's standard Makefile infrastructure drives Verilator;
  * every pyUVM test is run behind a hard OS timeout;
  * a structured simulation_result.json is emitted for the repair loop.

The generated verification environment is expected to contain:
  * generation_manifest.yaml
  * test_top.py
  * the generated pyUVM components
  * the Python reference model imported by the generated scoreboard

No SystemVerilog UVM package or DPI reference-model adapter is required.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

import yaml


DEFAULT_TIMEOUT = 60
DEFAULT_BUILD_TIMEOUT = 1800
MAX_LOG_CHARS = 12000


# ---------------------------------------------------------------------------
# Generic command helpers
# ---------------------------------------------------------------------------

def run_cmd(
    cmd: list[str],
    cwd: Path,
    timeout: int,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a command and capture stdout/stderr."""
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env if env is not None else os.environ.copy(),
    )


def tail(text: str, limit: int = MAX_LOG_CHARS) -> str:
    """Keep structured logs from becoming excessively large."""
    return text[-limit:] if len(text) > limit else text


# ---------------------------------------------------------------------------
# Manifest handling
# ---------------------------------------------------------------------------

def load_manifest(tb_dir: Path) -> dict[str, Any]:
    """Load generation_manifest.yaml from the generated TB."""
    path = tb_dir / "generation_manifest.yaml"

    if not path.exists():
        raise RuntimeError(f"Missing generation manifest: {path}")

    try:
        data = yaml.safe_load(path.read_text())
    except yaml.YAMLError as exc:
        raise RuntimeError(
            f"Invalid generation_manifest.yaml: {exc}"
        ) from exc

    if not isinstance(data, dict):
        raise RuntimeError(
            "generation_manifest.yaml must contain a mapping"
        )

    return data


def validate_manifest(
    manifest: dict[str, Any],
    tb_dir: Path,
    workspace: Path,
) -> tuple[str, Path, list[Path], list[str]]:
    """Validate the generated manifest.

    RTL paths are resolved relative to the ARGUS workspace, not generated_tb.
    This is important because the manifest normally contains paths such as:

        examples/adder/adder.sv
    """

    top_module = manifest.get("top_module")
    top_file = manifest.get("top_file")
    compile_files = manifest.get("compile_files")
    tests = manifest.get("test_classes")

    if not top_module or not isinstance(top_module, str):
        raise RuntimeError(
            "Manifest field 'top_module' is required"
        )

    if not top_file or not isinstance(top_file, str):
        raise RuntimeError(
            "Manifest field 'top_file' is required"
        )

    if not isinstance(compile_files, list) or not compile_files:
        raise RuntimeError(
            "Manifest field 'compile_files' must be a non-empty list"
        )

    if not isinstance(tests, list) or not tests:
        raise RuntimeError(
            "Manifest field 'test_classes' must be a non-empty list"
        )

    workspace = workspace.resolve()

    def resolve_workspace_file(name: str) -> Path:
        path = (workspace / name).resolve()

        if workspace not in path.parents and path != workspace:
            raise RuntimeError(
                f"Manifest file escapes workspace: {name}"
            )

        if not path.exists():
            raise RuntimeError(
                f"Manifest references missing file: {name}"
            )

        if path.suffix.lower() not in {".sv", ".v", ".vh", ".svh"}:
            raise RuntimeError(
                f"compile_files contains unsupported HDL file: {name}"
            )

        return path

    top_path = resolve_workspace_file(top_file)

    files: list[Path] = []
    for item in compile_files:
        files.append(resolve_workspace_file(str(item)))

    test_names = [
        str(item).strip()
        for item in tests
        if str(item).strip()
    ]

    python_test_module = str(manifest.get("python_test_module", "")).strip()
    if not python_test_module:
        raise RuntimeError(
            "Manifest field 'python_test_module' is required for Cocotb+pyUVM"
        )

    if not test_names:
        raise RuntimeError(
            "Manifest contains no runnable tests"
        )

    return top_module, top_path, files, test_names, python_test_module


# ---------------------------------------------------------------------------
# Generated TB validation / compatibility diagnostics
# ---------------------------------------------------------------------------

def detect_compatibility_findings(
    tb_dir: Path,
) -> list[dict[str, str]]:
    """Detect common generated-TB compatibility problems.

    These are diagnostics only.  They do not modify the generated TB.
    """

    findings: list[dict[str, str]] = []

    for path in sorted(tb_dir.glob("*.sv")):
        text = path.read_text(errors="replace")

        text = re.sub(
            r"//.*?$",
            "",
            text,
            flags=re.MULTILINE,
        )

        text = re.sub(
            r"/\*.*?\*/",
            "",
            text,
            flags=re.DOTALL,
        )

        if re.search(
            r"virtual\s+[A-Za-z_]\w*\s*#\s*\(",
            text,
        ):
            findings.append(
                {
                    "code": "parameterized_virtual_interface",
                    "file": path.name,
                    "message": (
                        "Parameterized virtual-interface type detected. "
                        "Use a concrete virtual interface type for "
                        "Verilator compatibility."
                    ),
                }
            )

        if re.search(
            r"uvm_config_db\s*#\s*\(\s*virtual\s+[A-Za-z_]\w*\s*#",
            text,
        ):
            findings.append(
                {
                    "code": (
                        "parameterized_config_db_virtual_interface"
                    ),
                    "file": path.name,
                    "message": (
                        "uvm_config_db is specialized with a "
                        "parameterized virtual-interface type."
                    ),
                }
            )

    return findings


# ---------------------------------------------------------------------------
# Log classification
# ---------------------------------------------------------------------------

def classify_build_error(log: str) -> str:
    lower = log.lower()

    if "cocotb" in lower and (
        "modulenotfounderror" in lower
        or "no module named" in lower
    ):
        return "cocotb_environment_error"

    if "pyuvm" in lower and (
        "modulenotfounderror" in lower
        or "no module named" in lower
    ):
        return "pyuvm_environment_error"

    if "make: ***" in lower:
        if "%error" in lower or "syntax error" in lower:
            return "compile_or_elaboration_error"

    if "internal fault" in lower:
        return "verilator_internal_fault"

    if "corrupted size" in lower:
        return "verilator_internal_fault"

    if "mismatching next->prev_size" in lower:
        return "verilator_internal_fault"

    if "undefined reference" in lower:
        return "link_error"

    if re.search(r"%Error", log):
        return "compile_or_elaboration_error"

    if "syntax error" in lower:
        return "compile_or_elaboration_error"

    if "error:" in lower:
        return "compile_or_elaboration_error"

    return "unknown_build_error"


def classify_runtime(
    log: str,
    returncode: int,
    timed_out: bool,
) -> str:
    if timed_out:
        return "hang"

    if "UVM_FATAL" in log:
        return "uvm_fatal"

    if "UVM_ERROR" in log:
        return "uvm_error"

    if "ERROR" in log and "Traceback" in log:
        return "runtime_error"

    if returncode != 0:
        return "runtime_error"

    return "pass"


def parse_counts(log: str) -> dict[str, int]:
    def count(pattern: str) -> int:
        return len(re.findall(pattern, log))

    return {
        "uvm_errors": count(r"\bUVM_ERROR\b"),
        "uvm_fatals": count(r"\bUVM_FATAL\b"),
        "uvm_warnings": count(r"\bUVM_WARNING\b"),
        "scoreboard_mismatches": count(
            r"\bmismatch\b"
        ),
        "assertion_failures": count(
            r"\bassert(?:ion)?[_ ]?(?:fail|failure|failed)\b"
        ),
    }


# ---------------------------------------------------------------------------
# Cocotb installation discovery
# ---------------------------------------------------------------------------

def find_cocotb_makefiles() -> Path:
    """Find Cocotb's installed Makefile.sim.

    Cocotb 2.x does not provide cocotb.runner.  Its simulator Makefile
    infrastructure is provided by cocotb_tools instead.
    """

    if not shutil.which("python3"):
        raise RuntimeError(
            "python3 not found in simulation container"
        )

    try:
        result = subprocess.run(
            [
                "python3",
                "-m",
                "cocotb_tools.config",
                "--makefiles",
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except Exception as exc:
        raise RuntimeError(
            f"Unable to query Cocotb installation: {exc}"
        ) from exc

    if result.returncode != 0:
        raise RuntimeError(
            "Unable to locate Cocotb Makefiles:\n"
            + tail(result.stdout + result.stderr)
        )

    makefiles = Path(result.stdout.strip())

    if not makefiles.exists():
        raise RuntimeError(
            f"Cocotb Makefile directory does not exist: {makefiles}"
        )

    makefile_sim = makefiles / "Makefile.sim"

    if not makefile_sim.exists():
        raise RuntimeError(
            f"Cocotb Makefile.sim not found: {makefile_sim}"
        )

    return makefiles


# ---------------------------------------------------------------------------
# Temporary Cocotb Makefile
# ---------------------------------------------------------------------------

def create_cocotb_makefile(
    sim_dir: Path,
    cocotb_makefiles: Path,
) -> Path:
    """Create the worker-owned Makefile used to launch Cocotb.

    This file lives under .chia_sim and is therefore not part of the
    persistent generated verification environment.
    """

    sim_dir.mkdir(parents=True, exist_ok=True)

    makefile = sim_dir / "Makefile"

    text = f"""# Auto-generated by workers/sim/run.py.
# This is a simulator launcher, not part of generated_tb.

SIM ?= verilator

TOPLEVEL_LANG ?= verilog

# IMPORTANT:
# Keep these as Makefile assignments rather than command-line Make
# variables.  Cocotb's Verilator backend appends its own required
# arguments (including --prefix Vtop -o Vtop).  A command-line
# COMPILE_ARGS=... would override those += assignments and cause
# Verilator to generate V<top>.mk while Cocotb tries to build Vtop.mk.
COMPILE_ARGS += --timing -Wno-fatal

# Per-test DUT parameter overrides (-G...), passed as CHIA_DUT_PARAMS on the
# make command line. Not EXTRA_ARGS: Cocotb also hands that to the sim binary.
COMPILE_ARGS += $(CHIA_DUT_PARAMS)

include {cocotb_makefiles / "Makefile.sim"}
"""

    makefile.write_text(text)

    return makefile


# ---------------------------------------------------------------------------
# Cocotb / pyUVM execution
# ---------------------------------------------------------------------------

_TEST_BLOCK_RE = re.compile(
    r"^\s*ifeq\s*\(\s*\$\((?:TEST|UVM_TESTNAME|TESTCASE)\)\s*,\s*([A-Za-z_]\w*)\s*\)(.*?)^\s*endif\b",
    re.M | re.S,
)
_PARAM_OVERRIDE_RE = re.compile(r"(?<!\S)-G[A-Za-z_]\w*=[^\s#]+")


def per_test_parameter_overrides(tb_dir: Path, test_name: str) -> list[str]:
    """Verilator -G overrides the generated TB Makefile declares for one test."""
    # The worker builds with its own launcher Makefile, so per-test DUT
    # geometry declared in the generated TB Makefile is otherwise ignored and
    # wide-parameter tests run against a default-parameter elaboration.
    makefile = tb_dir / "Makefile"
    if not makefile.is_file():
        return []
    overrides: list[str] = []
    for name, body in _TEST_BLOCK_RE.findall(makefile.read_text(errors="replace")):
        if name == test_name:
            overrides.extend(_PARAM_OVERRIDE_RE.findall(body))
    return list(dict.fromkeys(overrides))


def run_cocotb_test(
    makefile: Path,
    sim_dir: Path,
    tb_dir: Path,
    rtl: Path,
    top_module: str,
    top_file: Path,
    compile_files: list[Path],
    test_module: str,
    test_name: str,
    timeout_s: int,
    build_timeout: int,
) -> dict[str, Any]:
    """Run one pyUVM test through Cocotb + Verilator."""

    if not shutil.which("make"):
        return {
            "status": "fail",
            "category": "toolchain_missing_make",
            "timed_out": False,
            "returncode": None,
            "counts": {},
            "clean_exit": False,
            "log": "make not found",
            "command": [],
        }

    if not shutil.which("verilator"):
        return {
            "status": "fail",
            "category": "toolchain_missing_verilator",
            "timed_out": False,
            "returncode": None,
            "counts": {},
            "clean_exit": False,
            "log": "verilator not found",
            "command": [],
        }

    # Use a build directory unique to the RTL being simulated.  This prevents
    # Verilator artifacts compiled for the benchmark RTL from being reused
    # when an RTL-repair candidate is evaluated.
    #
    # Cocotb/Verilator owns the build directory.  It is intentionally outside
    # generated_tb so that the generated verification environment remains
    # clean and persistent.
    import hashlib
    rtl_key = hashlib.sha256(str(rtl.resolve()).encode("utf-8")).hexdigest()[:12]
    parameter_overrides = per_test_parameter_overrides(tb_dir, test_name)
    if parameter_overrides:
        params_key = hashlib.sha256(
            " ".join(sorted(parameter_overrides)).encode("utf-8")
        ).hexdigest()[:8]
        build_dir = sim_dir / "build" / f"{rtl_key}_{params_key}"
    else:
        build_dir = sim_dir / "build" / rtl_key
    results_file = sim_dir / f"{test_name}.xml"

    build_dir.mkdir(parents=True, exist_ok=True)

    # Remove an old result from a previous invocation.
    results_file.unlink(missing_ok=True)

    # Cocotb expects the Python module importable from PYTHONPATH.
    #
    # The benchmark TB directory itself is added to PYTHONPATH so the generated
    # top-level module can always be loaded as `test_top`, regardless of where
    # the benchmark output parent is configured.
    workspace = Path("/workspace").resolve()

    env = os.environ.copy()

    python_paths = [
        str(workspace),
        str(tb_dir),
        str(tb_dir.parent),
    ]

    if env.get("PYTHONPATH"):
        python_paths.append(env["PYTHONPATH"])

    env["PYTHONPATH"] = os.pathsep.join(python_paths)

    # This is the important bridge between the manifest test name and the
    # pyUVM test factory selection performed inside generated_tb/test_top.py.
    env["UVM_TESTNAME"] = test_name

    # Cocotb configuration.
    env["TOPLEVEL"] = top_module
    env["COCOTB_TOPLEVEL"] = top_module
    env["MODULE"] = test_module
    env["COCOTB_TEST_MODULES"] = test_module

    # Keep the benchmark-local generated TB as the working Python package.
    env["COCOTB_TEST_DIR"] = str(workspace)

    # Let Cocotb write its xUnit/JUnit-style result file somewhere controlled
    # by the worker.
    env["COCOTB_RESULTS_FILE"] = str(results_file)

    # Explicit HDL timing.
    env["COCOTB_HDL_TIMEUNIT"] = "1ns"
    env["COCOTB_HDL_TIMEPRECISION"] = "1ps"

    # Build with all available CPUs.  COMPILE_ARGS is deliberately NOT
    # supplied through the environment or command line; it is defined with
    # += in the temporary Makefile so Cocotb can append its required
    # Verilator/VPI arguments.
    env["BUILD_ARGS"] = "-j 4"

    # Keep waves disabled unless explicitly requested.  The worker's primary
    # output is the structured verification result.
    env.setdefault("WAVES", "0")

    # Cocotb's Makefile infrastructure uses VERILOG_SOURCES.
    #
    # The generated Python TB is NOT passed as an HDL source.
    # The explicit --rtl argument is authoritative for the DUT being
    # simulated.  The generation manifest may still contain the benchmark
    # RTL path from TB generation, so do not rely on compile_files alone.
    #
    # Keep all other manifest HDL files because they may be required support
    # sources, but replace the manifest's DUT/top RTL with the RTL supplied
    # by the caller.  This allows the same generated TB to verify:
    #   * the benchmark RTL during normal TB validation, and
    #   * an isolated repaired RTL candidate during RTL verification.
    # The explicit --rtl argument is authoritative.  The generated manifest
    # describes the RTL that existed when the TB was generated and may still
    # point at the benchmark DUT.  It must never cause the benchmark DUT to be
    # compiled alongside a repaired candidate.
    #
    # Start with exactly the requested DUT.
    source_paths = [str(rtl)]

    rtl_resolved = rtl.resolve()
    top_resolved = top_file.resolve()

    # The manifest can be inconsistent about which field identifies the DUT:
    # in some generated TBs the benchmark DUT appears in compile_files even
    # when top_file points somewhere else.  Therefore exclude:
    #   1. the explicit RTL itself (avoid duplicates),
    #   2. manifest top_file,
    #   3. any other HDL file that declares the requested top module.
    #
    # Remaining HDL files are treated as support sources and are retained.
    top_module_pattern = re.compile(
        rf"\\bmodule\\s+(?:\\w+\\s*#\\s*\\([^;]*?\\)\\s*)?{re.escape(top_module)}\\b"
        rf"|\\bmodule\\s+{re.escape(top_module)}\\b",
        re.DOTALL,
    )

    for path in compile_files:
        path_resolved = path.resolve()

        if path_resolved in {rtl_resolved, top_resolved}:
            continue

        try:
            source_text = path.read_text(errors="replace")
        except OSError:
            source_text = ""

        # Never compile another source that defines the DUT top module.
        # This specifically catches a benchmark RTL left in compile_files
        # under a path different from manifest top_file.
        if source_text and top_module_pattern.search(source_text):
            continue

        source_paths.append(str(path))

    verilator_sources = " ".join(source_paths)

    cmd = [
        "timeout",
        "--signal=TERM",
        "--kill-after=5",
        str(timeout_s),
        "make",
        "-f",
        str(makefile),
        f"SIM=verilator",
        "TOPLEVEL_LANG=verilog",
        f"TOPLEVEL={top_module}",
        f"COCOTB_TOPLEVEL={top_module}",
        f"MODULE={test_module}",
        f"COCOTB_TEST_MODULES={test_module}",
        f"VERILOG_SOURCES={verilator_sources}",
        f"SIM_BUILD={build_dir}",
        f"COCOTB_RESULTS_FILE={results_file}",
        "COCOTB_HDL_TIMEUNIT=1ns",
        "COCOTB_HDL_TIMEPRECISION=1ps",
        "BUILD_ARGS=-j 4",
    ]
    if parameter_overrides:
        cmd.append(f"CHIA_DUT_PARAMS={' '.join(parameter_overrides)}")

    # Keep the worker-side subprocess timeout slightly larger than the GNU
    # timeout so the timeout command can perform its TERM/KILL sequence.
    subprocess_timeout = max(
        timeout_s + 15,
        build_timeout + 15,
    )

    # The larger build timeout should be respected when compiling.
    #
    # GNU timeout is still bounded by timeout_s because the generated
    # simulation itself must not run indefinitely.
    #
    # If compilation is unusually slow, the Python subprocess timeout is
    # additionally bounded by build_timeout.
    subprocess_timeout = max(
        timeout_s + 15,
        min(build_timeout + timeout_s + 15, build_timeout + 300),
    )

    try:
        result = run_cmd(
            cmd,
            cwd=tb_dir,
            timeout=subprocess_timeout,
            env=env,
        )

    except subprocess.TimeoutExpired as exc:
        log = tail(
            (exc.stdout or "")
            + (exc.stderr or "")
        )

        return {
            "status": "hang",
            "category": "worker_timeout",
            "timed_out": True,
            "watchdog_expired": True,
            "returncode": None,
            "counts": parse_counts(log),
            "clean_exit": False,
            "log": log,
            "command": cmd,
            "test_name": test_name,
            "results_file": str(results_file),
        }

    except Exception as exc:
        return {
            "status": "fail",
            "category": "cocotb_invocation_error",
            "timed_out": False,
            "returncode": None,
            "counts": {},
            "clean_exit": False,
            "log": str(exc),
            "command": cmd,
            "test_name": test_name,
        }

    log = result.stdout + result.stderr

    # GNU timeout returns:
    #
    #   124 = command timed out
    #   137 = SIGKILL after escalation
    #
    timed_out = result.returncode in {124, 137}

    status = classify_runtime(
        log,
        result.returncode,
        timed_out,
    )

    # Cocotb may return a non-zero status while the useful diagnostic is
    # actually a Python exception / pyUVM failure.  Keep the complete log
    # available for Stage 7 rather than attempting to hide it.
    counts = parse_counts(log)

    return {
        "status": status,
        "category": "timeout" if timed_out else status,
        "timed_out": timed_out,
        "watchdog_expired": timed_out,
        "returncode": result.returncode,
        "counts": counts,
        "clean_exit": (
            not timed_out
            and result.returncode == 0
        ),
        "log": tail(log),
        "command": cmd,
        "test_name": test_name,
        "test_module": test_module,
        "dut_parameter_overrides": parameter_overrides,
        "results_file": str(results_file),
        "results_file_exists": results_file.exists(),
    }


# ---------------------------------------------------------------------------
# Main worker
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser()

    ap.add_argument(
        "--rtl",
        required=True,
    )

    ap.add_argument(
        "--tb",
        required=True,
    )

    ap.add_argument(
        "--output",
        required=True,
    )

    ap.add_argument(
        "--test-timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
    )

    ap.add_argument(
        "--build-timeout",
        type=int,
        default=DEFAULT_BUILD_TIMEOUT,
    )

    args = ap.parse_args()

    rtl = Path(args.rtl).resolve()
    tb_dir = Path(args.tb).resolve()
    output = Path(args.output).resolve()

    # All simulator-generated artifacts live here.
    #
    # generated_tb itself remains the persistent LLM-generated verification
    # environment.
    sim_dir = tb_dir / ".chia_sim"

    result: dict[str, Any] = {
        "schema_version": "1.0",
        "status": "fail",
        "rtl": str(rtl),
        "tb_dir": str(tb_dir),
        "toolchain": {
            "simulator": "verilator",
            "cocotb": None,
            "pyuvm": None,
        },
        "tests": {},
    }

    try:
        # ---------------------------------------------------------------
        # Basic validation
        # ---------------------------------------------------------------

        if not rtl.exists():
            raise RuntimeError(
                f"RTL not found: {rtl}"
            )

        if not tb_dir.is_dir():
            raise RuntimeError(
                f"TB directory not found: {tb_dir}"
            )

        # Generated Cocotb entry point.
        test_top = tb_dir / "test_top.py"

        if not test_top.exists():
            raise RuntimeError(
                f"Generated Cocotb entry point missing: {test_top}"
            )

        # ---------------------------------------------------------------
        # Load and validate manifest
        # ---------------------------------------------------------------

        manifest = load_manifest(tb_dir)

        workspace = Path("/workspace").resolve()

        (
            top_module,
            top_file,
            compile_files,
            tests,
            test_module,
        ) = validate_manifest(
            manifest,
            tb_dir,
            workspace,
        )

        result["manifest"] = manifest
        result["top_module"] = top_module
        result["top_file"] = str(top_file)
        result["compile_files"] = [
            str(path)
            for path in compile_files
        ]
        result["simulation_rtl_authoritative"] = str(rtl)
        result["test_classes"] = tests

        # ---------------------------------------------------------------
        # Check Cocotb / pyUVM environment
        # ---------------------------------------------------------------

        if not shutil.which("python3"):
            raise RuntimeError(
                "python3 not found"
            )

        cocotb_check = subprocess.run(
            [
                "python3",
                "-c",
                "import cocotb; print(cocotb.__version__)",
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )

        if cocotb_check.returncode != 0:
            raise RuntimeError(
                "Cocotb is not importable:\n"
                + tail(
                    cocotb_check.stdout
                    + cocotb_check.stderr
                )
            )

        pyuvm_check = subprocess.run(
            [
                "python3",
                "-c",
                "import pyuvm; print(pyuvm.__version__)",
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )

        if pyuvm_check.returncode != 0:
            raise RuntimeError(
                "pyUVM is not importable:\n"
                + tail(
                    pyuvm_check.stdout
                    + pyuvm_check.stderr
                )
            )

        result["toolchain"]["cocotb"] = (
            cocotb_check.stdout.strip()
        )

        result["toolchain"]["pyuvm"] = (
            pyuvm_check.stdout.strip()
        )

        result["toolchain"]["verilator"] = (
            shutil.which("verilator")
        )

        # ---------------------------------------------------------------
        # Locate Cocotb's standard Makefile infrastructure
        # ---------------------------------------------------------------

        cocotb_makefiles = find_cocotb_makefiles()

        makefile = create_cocotb_makefile(
            sim_dir,
            cocotb_makefiles,
        )

        result["cocotb"] = {
            "makefiles": str(cocotb_makefiles),
            "makefile": str(makefile),
            "test_module": test_module,
        }

        # ---------------------------------------------------------------
        # Compatibility diagnostics
        # ---------------------------------------------------------------

        result["compatibility_findings"] = (
            detect_compatibility_findings(tb_dir)
        )

        # ---------------------------------------------------------------
        # Run every manifest test
        # ---------------------------------------------------------------

        test_module = str(manifest.get("python_test_module", "")).strip()

        for test_name in tests:
            test_result = run_cocotb_test(
                makefile=makefile,
                sim_dir=sim_dir,
                tb_dir=tb_dir,
                rtl=rtl,
                top_module=top_module,
                top_file=top_file,
                compile_files=compile_files,
                test_module=test_module,
                test_name=test_name,
                timeout_s=args.test_timeout,
                build_timeout=args.build_timeout,
            )

            result["tests"][test_name] = test_result

        # ---------------------------------------------------------------
        # Aggregate result
        # ---------------------------------------------------------------

        test_results = list(
            result["tests"].values()
        )

        if (
            test_results
            and all(
                item["status"] == "pass"
                and item["clean_exit"]
                for item in test_results
            )
        ):
            result["status"] = "pass"

        elif any(
            item["status"] == "hang"
            for item in test_results
        ):
            result["status"] = "hang"

        elif any(
            item["status"]
            in {
                "uvm_error",
                "uvm_fatal",
                "runtime_error",
            }
            for item in test_results
        ):
            result["status"] = "functional_failure"

        elif any(
            item["status"] == "fail"
            for item in test_results
        ):
            result["status"] = "fail"

        else:
            result["status"] = "fail"

    except Exception as exc:
        result["status"] = "validation_error"
        result["error"] = str(exc)

    # ---------------------------------------------------------------
    # Always emit structured JSON
    # ---------------------------------------------------------------

    output.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    output.write_text(
        json.dumps(
            result,
            indent=2,
        )
        + "\n"
    )

    print(f"Wrote {output}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())