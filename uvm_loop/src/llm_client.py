"""
Reusable OpenCode LLM client with configurable providers, driven by the ARGUS
orchestration layer.

The default is Google Gemini through OpenCode. The OpenCode model ID
remains configurable through LLM_MODEL so existing OpenCode/OpenRouter
models can still be selected without changing pipeline code.

The public interface is intentionally small so both verification-plan
generation and UVM-generation can use the same client.
"""

from __future__ import annotations

import json
import os
import re
from typing import Any, Sequence

import yaml
from chia.base.ChiaFunction import get
from chia.models.opencode import OpenCodeLLM


CONTAINER_WORKSPACE = os.environ.get("CHIA_WORKSPACE", "/workspace")


def _load_model_name() -> str:
    """Resolve the OpenCode model from environment/config."""
    model = os.environ.get("LLM_MODEL")
    if model:
        return model

    config_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "config",
        "config.yaml",
    )

    try:
        with open(config_path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}

        return cfg.get("llm", {}).get(
            "default_model",
            "google/gemini-2.5-flash",
        )
    except Exception:
        return "google/gemini-2.5-flash"


def _load_int_env(name: str, default: int) -> int:
    """Read a positive integer environment variable safely."""
    value = os.environ.get(name)

    if value is None:
        return default

    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(
            f"{name} must be an integer, got {value!r}"
        ) from exc

    if parsed <= 0:
        raise ValueError(
            f"{name} must be greater than zero, got {parsed}"
        )

    return parsed


def _strip_code_fences(text: str) -> str:
    """Remove optional Markdown code fences from an LLM response."""
    text = text.strip()

    text = re.sub(
        r"^```(?:json)?\s*",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"\s*```$",
        "",
        text,
    )

    return text.strip()


def _extract_json(text: str) -> str:
    """
    Extract a JSON object/array when the model surrounds it with
    incidental text.

    We still strongly instruct the model to return JSON only. This
    fallback simply makes the client more tolerant of harmless
    surrounding text.
    """
    cleaned = _strip_code_fences(text)

    try:
        json.loads(cleaned)
        return cleaned
    except json.JSONDecodeError:
        pass

    object_start = cleaned.find("{")
    object_end = cleaned.rfind("}")

    if object_start >= 0 and object_end > object_start:
        candidate = cleaned[object_start : object_end + 1]
        try:
            json.loads(candidate)
            return candidate
        except json.JSONDecodeError:
            pass

    array_start = cleaned.find("[")
    array_end = cleaned.rfind("]")

    if array_start >= 0 and array_end > array_start:
        candidate = cleaned[array_start : array_end + 1]
        try:
            json.loads(candidate)
            return candidate
        except json.JSONDecodeError:
            pass

    return cleaned


class LLMClient:
    """
    Thin reusable interface over the framework's OpenCodeLLM. The selected model
    may be Google Gemini, OpenRouter, OpenCode, or another provider supported
    by the installed OpenCode version.

    The rest of the pipeline should not need to know whether the model
    is OpenCode, Claude, GPT, etc. The backend configuration lives here.
    """

    def __init__(
        self,
        model: str | None = None,
        max_tokens: int = 4096,
        temperature: float = 0.2,
        timeout_seconds: int | None = None,
        retries: int | None = None,
    ):
        self.model = model or _load_model_name()
        self.max_tokens = max_tokens
        self.temperature = temperature

        self.timeout_seconds = (
            timeout_seconds
            if timeout_seconds is not None
            else _load_int_env("LLM_TIMEOUT", 600)
        )

        self.retries = (
            retries
            if retries is not None
            else _load_int_env("LLM_RETRIES", 1)
        )

        self.llm = OpenCodeLLM(
            model=self.model,
            work_dir=CONTAINER_WORKSPACE,
            timeout_seconds=self.timeout_seconds,
            retries=self.retries,
        )

    def generate_text(
        self,
        system_prompt: str,
        user_prompt: str,
        tools: Sequence[Any] | None = None,
    ) -> str:
        """
        Execute an OpenCode request and return its text.

        Raises RuntimeError with the OpenCode response details when
        the remote request fails instead of hiding the actual failure.
        """

        prompt = f"""
{system_prompt}

IMPORTANT:
Follow the requested output format exactly.
Do not add explanations outside the requested output.

USER REQUEST:
{user_prompt}
""".strip()

        kwargs: dict[str, Any] = {}

        if tools:
            kwargs["tools"] = list(tools)

        response = get(
            self.llm.prompt.chia_remote(
                self.llm,
                prompt,
                **kwargs,
            )
        )

        if not getattr(response, "success", True):
            result = getattr(response, "result", "")
            error = getattr(response, "error", None)

            details = error or result or "No error details returned."

            raise RuntimeError(
                "OpenCode generation failed "
                f"(model={self.model}, "
                f"timeout={self.timeout_seconds}s, "
                f"retries={self.retries}):\n{details}"
            )

        raw = getattr(response, "result", None)

        if raw is None:
            raise RuntimeError(
                "OpenCode returned no result "
                f"(model={self.model})."
            )

        raw = str(raw).strip()

        if not raw:
            raise RuntimeError(
                "OpenCode returned an empty result "
                f"(model={self.model})."
            )

        return raw

    def generate_json(
        self,
        system_prompt: str,
        user_prompt: str,
        tools: Sequence[Any] | None = None,
    ) -> dict:
        """
        Execute OpenCode and parse the response as a JSON object.
        """

        raw = self.generate_text(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            tools=tools,
        )

        cleaned = _extract_json(raw)

        try:
            parsed = json.loads(cleaned)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "OpenCode did not return valid JSON "
                f"({exc}).\n\nRaw response:\n{raw}"
            ) from exc

        if not isinstance(parsed, dict):
            raise ValueError(
                "Expected the LLM to return a JSON object, "
                f"got {type(parsed).__name__}.\n\n"
                f"Raw response:\n{raw}"
            )

        return parsed
