"""
LLM-assisted UVM generation planner.

This module does NOT create its own LLM client or Ray/OpenCode connection.

The ARGUS orchestration layer owns the OpenCodeLLM instance and passes an LLM
callable into generate_generation_spec(). This keeps LLM execution inside the
framework's existing OpenCode worker while keeping generation deterministic
after the GenerationSpec has been produced.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Callable

import yaml

from src.schema import VerificationPlan

from .generation_schema import GenerationSpec
from .validator import validate_generation_spec


ROOT = Path(__file__).resolve().parent

CAPS = yaml.safe_load(
    (ROOT / "capabilities.yaml").read_text()
)

RULES = yaml.safe_load(
    (ROOT / "rules.yaml").read_text()
)


SYSTEM = """
You are the ARGUS UVM environment generation planner.

Return ONLY one JSON object matching the supplied GenerationSpec schema.

You are NOT generating SystemVerilog source code.

Your task is to translate the accepted VerificationPlan into a
GenerationSpec that selects and configures only supported UVM generation
capabilities.

HARD RULES:

1. Never invent DUT ports, parameters, clocks, resets, or signals.
2. Preserve the DUT facts from the accepted VerificationPlan.
3. Never invent reference-model functions, arguments, return values, or
   algorithms.
4. The supplied Python reference model is the behavioral oracle.
5. Do not reproduce the Python reference-model algorithm in SystemVerilog.
6. Do not generate SystemVerilog source code in any GenerationSpec field.
7. Use ONLY capabilities present in capabilities.yaml.
8. Follow ALL rules in rules.yaml.
9. If the requested verification behavior cannot be represented using the
   supported capabilities, fail closed rather than inventing a capability.
10. Explicitly map DUT inputs, DUT outputs, reference-model arguments,
    reference-model return values, scoreboard inputs, and scoreboard outputs
    whenever those concepts exist in the GenerationSpec schema.
11. Scoreboard expected values must come from the supplied Python reference
    model, not from an LLM prediction or duplicated algorithm.
12. Preserve the latency established by RTL/source evidence.
13. Do not assume the DUT is an adder or make any design-specific assumptions
    that are not supported by the supplied sources.
14. The target simulator is Verilator 5.042 with UVM 1800.2-2020.3.1.
15. Do not generate parameterized virtual interface types. The generated
    interface must have concrete signal widths from the validated plan, and
    UVM components must use `virtual <dut>_if` without `#(...)`.
16. If an interface is propagated through UVM config_db, specialize it as
    `uvm_config_db#(virtual <dut>_if)`, never as
    `uvm_config_db#(virtual <dut>_if#(...))`.
17. The generated tb_top must use the same non-parameterized virtual-interface
    type in its config_db set.
18. Do not remove UVM_NO_DPI, the simulation timeout wrapper, or the watchdog
    to work around simulator behavior; those are fixed toolchain requirements.
"""


def build_prompt(
    plan: VerificationPlan,
    rtl_path: str,
    spec_path: str,
    ref_model_path: str,
) -> str:
    """
    Build the complete GenerationSpec planning prompt.

    All source material is provided to the LLM so that generation decisions
    remain grounded in the RTL, specification, accepted verification plan,
    capabilities, and generation rules.
    """

    rtl = Path(rtl_path).read_text()
    spec = Path(spec_path).read_text()
    ref_model = Path(ref_model_path).read_text()

    generation_schema = GenerationSpec.model_json_schema()

    return f"""
## CAPABILITIES

{yaml.safe_dump(CAPS, sort_keys=False)}

## GENERATOR RULES

{yaml.safe_dump(RULES, sort_keys=False)}

## GENERATION SCHEMA

{json.dumps(generation_schema, indent=2)}

## ACCEPTED VERIFICATION PLAN

{json.dumps(plan.model_dump(mode="json"), indent=2)}

## RTL

```systemverilog
{rtl}
```

## SPECIFICATION

{spec}

## PYTHON REFERENCE MODEL

```python
{ref_model}
```

## VERILATOR/UVM COMPATIBILITY CONTRACT

The generated environment will be rendered and simulated with Verilator 5.042
and UVM 1800.2-2020.3.1. Follow these requirements exactly:

- UVM is compiled with +define+UVM_NO_DPI. Do not depend on UVM DPI.
- Simulation uses --timing and a hard OS timeout.
- tb_top must retain a deterministic watchdog that calls $finish and emits
  CHIA_WATCHDOG_EXPIRED if the simulation does not terminate normally.
- Do NOT parameterize the generated interface type. Render concrete widths
  from the validated GenerationSpec.
- Declare virtual interfaces as `virtual <dut>_if`, never
  `virtual <dut>_if#(...)`.
- If uvm_config_db is used for the interface, use
  `uvm_config_db#(virtual <dut>_if)` on both set and get. Never specialize
  config_db with a parameterized virtual-interface type.
- Preserve this exact compatibility pattern consistently in tb_top, driver,
  monitor, scoreboard, and sequences.

These are hard simulator-compatibility constraints, not optional style choices.
The deterministic renderer also checks these patterns and will fail closed if
the generated source violates them.

## TASK

Create exactly one GenerationSpec JSON object.

The GenerationSpec must preserve the accepted plan and must be grounded in
the supplied RTL, specification, and Python reference model.

For every generated connection or mapping:

- use only real DUT signals;
- use the actual DUT clock/reset;
- preserve actual signal widths;
- preserve actual input/output directions;
- preserve the reference-model function and interface;
- map every required reference-model argument explicitly;
- map every reference-model return/output explicitly;
- configure the scoreboard to obtain expected values from the supplied
  Python reference model;
- do not implement a second prediction algorithm in SystemVerilog;
- preserve the latency supported by the RTL and plan;
- use only capabilities defined in capabilities.yaml;
- do not introduce unsupported UVM components or mechanisms.

For test scenarios, use concrete stimulus values only when they are present
in the accepted plan or supported by source evidence. Otherwise represent
the requested behavior using the supported constraint/randomization
mechanisms.

Return JSON only.
"""


def generate_generation_spec(
    plan: VerificationPlan,
    rtl_path: str,
    spec_path: str,
    ref_model_path: str,
    llm: Callable[[str, str], str],
) -> GenerationSpec:
    """
    Generate and validate a GenerationSpec using the provided LLM.

    The LLM callable is supplied by the ARGUS orchestration layer. This module
    does not own OpenCode, Ray, credentials, model selection, or worker
    placement.
    """

    if llm is None:
        raise ValueError(
            "An LLM callable must be supplied by the ARGUS orchestration layer."
        )

    raw = llm(
        SYSTEM,
        build_prompt(
            plan,
            rtl_path,
            spec_path,
            ref_model_path,
        ),
    )

    if not raw or not raw.strip():
        raise RuntimeError(
            "Generation planner returned an empty response."
        )

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            "Generation planner did not return valid JSON.\n\n"
            f"Raw response:\n{raw}"
        ) from exc

    spec = GenerationSpec.model_validate(data)

    validate_generation_spec(
        spec,
        plan,
        rtl_path,
        ref_model_path,
    )

    return spec
