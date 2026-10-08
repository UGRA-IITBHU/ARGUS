#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------
# ARGUS project setup
# ------------------------------------------------------------

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export CHIA_PROJECT_ROOT="$PROJECT_ROOT"
export THIS_MACHINE="$(hostname -I | awk '{print $1}')"

echo "========================================"
echo "ARGUS LLM-UVM-TB Generator Setup"
echo "========================================"
echo
echo "Project root : $CHIA_PROJECT_ROOT"
echo "This machine: $THIS_MACHINE"
echo

# ------------------------------------------------------------
# Basic checks
# ------------------------------------------------------------

if ! command -v chia >/dev/null 2>&1; then
    echo "ERROR: 'chia' command not found."
    echo "Activate your chia_env conda environment first."
    exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: Docker is not installed or not in PATH."
    exit 1
fi

if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker daemon is not accessible."
    exit 1
fi

# ------------------------------------------------------------
# LLM provider configuration
# ------------------------------------------------------------

echo "Select LLM provider:"
echo
echo "  1) OpenCode"
echo "  2) Google Gemini"
echo

read -r -p "Enter choice [1-2]: " LLM_CHOICE

case "$LLM_CHOICE" in

    1)
        # ----------------------------------------------------
        # OpenCode
        # ----------------------------------------------------
        export LLM_PROVIDER="opencode"

        echo
        read -r -p "Enter OpenCode model name: " LLM_MODEL

        if [[ -z "$LLM_MODEL" ]]; then
            echo
            echo "ERROR: OpenCode model name cannot be empty."
            exit 1
        fi

        export LLM_MODEL

        echo
        echo "LLM provider : OpenCode"
        echo "LLM model    : $LLM_MODEL"
        echo "Gemini API key is not required."
        echo
        ;;

    2)
        # ----------------------------------------------------
        # Google Gemini
        # ----------------------------------------------------
        export LLM_PROVIDER="google"

        echo
        read -r -p \
            "Enter Gemini model name [google/gemini-2.5-flash]: " \
            GEMINI_MODEL

        GEMINI_MODEL="${GEMINI_MODEL:-google/gemini-2.5-flash}"

        export LLM_MODEL="$GEMINI_MODEL"

        if [[ -z "${GOOGLE_GENERATIVE_AI_API_KEY:-}" ]]; then
            echo
            echo "ERROR: LLM_PROVIDER=google requires"
            echo "GOOGLE_GENERATIVE_AI_API_KEY."
            echo
            echo "Set it before running setup.sh:"
            echo
            echo '    export GOOGLE_GENERATIVE_AI_API_KEY="your-gemini-api-key"'
            echo
            exit 1
        fi

        echo
        echo "LLM provider : Google Gemini"
        echo "LLM model    : $LLM_MODEL"
        echo
        ;;

    *)
        echo
        echo "ERROR: Invalid choice."
        echo "Please select 1 for OpenCode or 2 for Google Gemini."
        exit 1
        ;;

esac

# ------------------------------------------------------------
# Check cluster configuration
# ------------------------------------------------------------

if [[ ! -f "$PROJECT_ROOT/cluster.yaml" ]]; then
    echo "ERROR: cluster.yaml not found."
    exit 1
fi

# ------------------------------------------------------------
# Check required local RTL worker image
# ------------------------------------------------------------

if ! docker image inspect chia-rtl-worker:local >/dev/null 2>&1; then
    echo "ERROR: chia-rtl-worker:local image not found."
    echo
    echo "Build it first, for example:"
    echo
    echo "    docker build -t chia-rtl-worker:local -f workers/rtl/Dockerfile ."
    echo
    exit 1
fi

# ------------------------------------------------------------
# Check simulation worker image
# ------------------------------------------------------------

if ! docker image inspect chia-sim-worker:chia-local >/dev/null 2>&1; then
    echo "ERROR: chia-sim-worker:chia-local image not found."
    echo
    echo "Build it first:"
    echo
    echo "    make build-sim-image"
    echo
    exit 1
fi

# ------------------------------------------------------------
# Export variables for the ARGUS cluster
# ------------------------------------------------------------

export CHIA_PROJECT_ROOT
export THIS_MACHINE
export LLM_PROVIDER
export LLM_MODEL

echo "Environment configured."
echo

# ------------------------------------------------------------
# Start the ARGUS cluster
# ------------------------------------------------------------

echo "Starting ARGUS cluster..."
echo

chia up "$PROJECT_ROOT/cluster.yaml"