#!/usr/bin/env python3
"""Generate a capability-driven UVM environment from an accepted plan."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import yaml

from src.schema import VerificationPlan
from uvm_generator.generation_schema import GenerationSpec
from uvm_generator.renderer import render
from uvm_generator.validator import validate_generation_spec


class TBGenerator:
    def generate(
        self,
        plan_path: str,
        generation_spec_path: str,
        rtl_path: str,
        spec_path: str,
        ref_model_path: str,
        out_dir: str = "generated_tb",
    ) -> str:
        """
        Generate a UVM environment from an accepted VerificationPlan and
        a pre-generated GenerationSpec.

        LLM/OpenCode execution is intentionally outside this class.
        run2.py produces GenerationSpec; this worker only validates
        the structured artifact and renders the UVM environment.
        """

        plan_path = str(Path(plan_path).resolve())
        generation_spec_path = str(
            Path(generation_spec_path).resolve()
        )

        plan_data = yaml.safe_load(
            Path(plan_path).read_text()
        )
        plan = VerificationPlan.model_validate(plan_data)

        generation_spec_data = json.loads(
            Path(generation_spec_path).read_text()
        )
        gen = GenerationSpec.model_validate(generation_spec_data)

        # Validate the GenerationSpec against the accepted plan and
        # source/reference-model constraints before rendering anything.
        validate_generation_spec(
            gen,
            plan,
            rtl_path,
            ref_model_path,
        )

        out_path = Path(out_dir)
        out_path.mkdir(parents=True, exist_ok=True)

        # Persist the validated structured intermediate artifact.
        persisted_spec = out_path / "generation_spec.json"
        persisted_spec.write_text(
            json.dumps(
                gen.model_dump(mode="json"),
                indent=2,
            )
        )

        # Deterministic rendering of the actual UVM environment.
        written = render(
            gen,
            str(out_path),
            rtl_path,
            plan_path,
            ref_model_path,
        )

        print(f"Wrote {len(written) + 1} files to {out_path}/")

        return str(out_path.resolve())


def load_plan(path: str) -> VerificationPlan:
    """Load and validate a canonical VerificationPlan."""
    return VerificationPlan.model_validate(
        yaml.safe_load(Path(path).read_text())
    )


def main() -> None:
    """CLI entry point used inside the ARGUS RTL/UVM worker."""

    parser = argparse.ArgumentParser(
        description=(
            "Generate a UVM testbench from an accepted verification "
            "plan and a validated GenerationSpec."
        )
    )

    parser.add_argument("--plan", required=True)
    parser.add_argument("--generation-spec", required=True)
    parser.add_argument("--rtl", required=True)
    parser.add_argument("--spec", required=True)
    parser.add_argument("--ref-model", required=True)
    parser.add_argument("--out-dir", default=None, help="Output TB directory")
    parser.add_argument("--benchmark", default=None, help="Benchmark name for isolated output")
    parser.add_argument("--out-parent", default="generated/designs", help="Parent for isolated benchmark output")

    args = parser.parse_args()

    if args.out_dir:
        output_dir = args.out_dir
    elif args.benchmark:
        output_dir = str(Path(args.out_parent) / args.benchmark / "tb")
    else:
        raise SystemExit("Specify --out-dir or use --benchmark")

    generator = TBGenerator()

    generator.generate(
        plan_path=args.plan,
        generation_spec_path=args.generation_spec,
        rtl_path=args.rtl,
        spec_path=args.spec,
        ref_model_path=args.ref_model,
        out_dir=output_dir,
    )


if __name__ == "__main__":
    main()
