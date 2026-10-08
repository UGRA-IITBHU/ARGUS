"""ARGUS functions for the verification-generation pipeline.

ARGUS remains the orchestration layer. Tool implementations are kept in the
containerized workers under ``workers/``.
"""

from pathlib import Path
import json
import subprocess

from chia.base.ChiaFunction import ChiaFunction


@ChiaFunction(resources={"rtl_extract": 1})
def extract_rtl(
    rtl_path: str,
    output_path: str = "generated/rtl/rtl_info.json",
) -> str:
    """Run RTL extraction inside the ARGUS RTL worker."""

    workspace = Path("/workspace")

    rtl = Path(rtl_path)
    output = Path(output_path)

    # Relative paths are resolved against the shared ARGUS workspace.
    if not rtl.is_absolute():
        rtl = workspace / rtl

    if not output.is_absolute():
        output = workspace / output

    rtl = rtl.resolve()
    output = output.resolve()

    if not rtl.exists():
        raise FileNotFoundError(f"RTL file not found: {rtl}")

    output.parent.mkdir(parents=True, exist_ok=True)

    subprocess.run(
        [
            "python3",
            "/opt/rtl-worker/entrypoint.py",
            "--rtl",
            str(rtl),
            "--output",
            str(output),
        ],
        check=True,
    )

    return str(output)


@ChiaFunction(resources={"rtl_extract": 1})
def generate_uvm_in_worker(
    plan_path: str,
    generation_spec_path: str,
    rtl_path: str,
    spec_path: str,
    ref_model_path: str,
    out_dir: str = "generated_tb",
) -> str:
    """Run deterministic UVM generation inside the ARGUS RTL worker."""

    workspace = Path("/workspace")

    def resolve_workspace_path(path: str) -> Path:
        p = Path(path)
        if not p.is_absolute():
            p = workspace / p
        return p.resolve()

    plan = resolve_workspace_path(plan_path)
    generation_spec = resolve_workspace_path(generation_spec_path)
    rtl = resolve_workspace_path(rtl_path)
    spec = resolve_workspace_path(spec_path)
    ref_model = resolve_workspace_path(ref_model_path)
    output = resolve_workspace_path(out_dir)

    for path, label in (
        (plan, "verification plan"),
        (generation_spec, "generation spec"),
        (rtl, "RTL"),
        (spec, "specification"),
        (ref_model, "reference model"),
    ):
        if not path.exists():
            raise FileNotFoundError(
                f"{label} not found: {path}"
            )

    output.mkdir(parents=True, exist_ok=True)

    subprocess.run(
        [
            "python3",
            "/opt/uvm-generator/scripts/generate_tb.py",
            "--plan",
            str(plan),
            "--generation-spec",
            str(generation_spec),
            "--rtl",
            str(rtl),
            "--spec",
            str(spec),
            "--ref-model",
            str(ref_model),
            "--out-dir",
            str(output),
        ],
        check=True,
    )

    return str(output)


@ChiaFunction(resources={"sim_worker": 1})
def simulate(
    rtl_path: str,
    tb_dir: str,
    output_path: str = "generated/results/simulation_result.json",
    test_timeout: int = 60,
    build_timeout: int = 1800,
) -> str:
    """Run the Verilator+UVM simulation worker on the generated TB."""
    workspace = Path("/workspace")

    def resolve(path: str) -> Path:
        p = Path(path)
        if not p.is_absolute():
            p = workspace / p
        return p.resolve()

    rtl = resolve(rtl_path)
    tb = resolve(tb_dir)
    output = resolve(output_path)

    if not rtl.exists():
        raise FileNotFoundError(f"RTL not found: {rtl}")
    if not tb.is_dir():
        raise FileNotFoundError(f"Generated TB directory not found: {tb}")

    output.parent.mkdir(parents=True, exist_ok=True)

    subprocess.run(
        [
            "python3",
            "/workspace/workers/sim/run.py",
            "--rtl", str(rtl),
            "--tb", str(tb),
            "--output", str(output),
            "--test-timeout", str(test_timeout),
            "--build-timeout", str(build_timeout),
        ],
        check=True,
    )
    return str(output)


def analyze_results(result_path: str) -> dict:
    """Convert simulation_result.json into a repair-loop decision."""
    data = json.loads(Path(result_path).read_text())
    status = data.get("status")

    if status == "pass":
        return {"verdict": "pass", "detail": data}
    if status in {"validation_error", "build_failure"}:
        build = data.get("build", {})
        category = build.get("category", data.get("error", "unknown"))
        template_categories = {
            "toolchain_missing_verilator",
            "toolchain_missing_uvm",
            "toolchain_or_uvm_dpi_error",
        }
        verdict = "template_bug" if category in template_categories else "generation_bug"
        return {"verdict": verdict, "detail": {"status": status, "category": category, "data": data}}

    tests = data.get("tests", {})
    if any(t.get("status") == "hang" for t in tests.values()):
        return {"verdict": "hang", "detail": {"status": status, "tests": tests}}
    if status == "functional_failure":
        return {"verdict": "generation_bug", "detail": {"status": status, "tests": tests}}
    return {"verdict": "generation_bug", "detail": data}
