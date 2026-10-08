"""Compatibility entry point for the current ARGUS pipeline.

The active end-to-end orchestration is implemented in ``pipeline.run4``.
This module intentionally does not expose the obsolete generate_plan/
generate_tb functions that existed in an earlier pipeline revision.
"""

from .functions import extract_rtl, simulate, analyze_results


def verification_loop(rtl_path: str, tb_dir: str):
    """Run the deterministic simulation/analysis portion of the pipeline.

    Full plan generation and LLM TB generation remain in ``pipeline.run4``.
    """
    result_path = simulate(rtl_path, tb_dir)
    return analyze_results(result_path)
