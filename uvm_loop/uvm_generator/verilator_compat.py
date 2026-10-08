from __future__ import annotations

import re
from pathlib import Path


PARAM_VIRTUAL_RE = re.compile(
    r"\bvirtual\s+[A-Za-z_]\w*\s*#\s*\(", re.MULTILINE
)
PARAM_CONFIG_RE = re.compile(
    r"uvm_config_db\s*#\s*\(\s*virtual\s+[A-Za-z_]\w*\s*#",
    re.MULTILINE,
)


def find_forbidden_patterns(tb_dir: str | Path) -> list[dict[str, str]]:
    root = Path(tb_dir)
    findings: list[dict[str, str]] = []
    for path in sorted(root.glob("*.sv")):
        text = path.read_text(errors="replace")
        # Ignore comments so documentation examples do not trip the check.
        text = re.sub(r"//.*?$", "", text, flags=re.MULTILINE)
        text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
        if PARAM_VIRTUAL_RE.search(text):
            findings.append({
                "code": "parameterized_virtual_interface",
                "file": path.name,
                "message": (
                    "Parameterized virtual-interface type detected. "
                    "Use a concrete-width interface and `virtual <dut>_if`."
                ),
            })
        if PARAM_CONFIG_RE.search(text):
            findings.append({
                "code": "parameterized_config_db_virtual_interface",
                "file": path.name,
                "message": (
                    "uvm_config_db is specialized with a parameterized virtual-interface "
                    "type; this is not supported by the ARGUS Verilator 5.042 flow."
                ),
            })
    return findings


def assert_verilator_compatible(tb_dir: str | Path) -> None:
    findings = find_forbidden_patterns(tb_dir)
    if findings:
        details = "\n".join(
            f"- {x['file']}: {x['code']}: {x['message']}" for x in findings
        )
        raise ValueError(
            "Generated TB violates the ARGUS Verilator compatibility contract:\n"
            + details
        )
