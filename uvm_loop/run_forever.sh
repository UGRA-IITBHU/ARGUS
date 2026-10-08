#!/usr/bin/env bash
# Generic supervisor for the ARGUS verification-improvement loop.
#
# Usage:
#   ./run_forever.sh <design-config> [max-iterations]
#
# Examples:
#   ./run_forever.sh benchmarks/fifo/design.yaml 25
#   ./run_forever.sh benchmarks/adder2/design.yaml 30
#
# The script is intentionally project/design agnostic:
#   - uses the current directory as the project root
#   - accepts the design config as an argument
#   - derives the design name from the YAML filename
#   - derives the log directory from generated/designs/<design>/...
#   - does not hard-code a username, absolute project path, or FIFO

set -u

if [[ $# -lt 1 || $# -gt 2 ]]; then
    echo "Usage: $0 <design-config> [max-iterations]"
    echo "Example: $0 benchmarks/fifo/design.yaml 25"
    exit 2
fi

DESIGN_CONFIG="$1"
MAX_ITERS="${2:-${MAX_VERIFICATION_IMPROVEMENT_ITERATIONS:-1000}}"
PROJECT_ROOT="$(pwd)"

if [[ ! -f "$DESIGN_CONFIG" ]]; then
    echo "ERROR: design config not found: $DESIGN_CONFIG"
    exit 1
fi

# Normalize a leading ./ for cleaner paths.
DESIGN_CONFIG="${DESIGN_CONFIG#./}"

# Derive the design name from the config filename.
DESIGN_FILE="$(basename "$DESIGN_CONFIG")"
DESIGN_NAME="${DESIGN_FILE%.*}"

LOG_DIR="$PROJECT_ROOT/generated/designs/$DESIGN_NAME/results/verification_improvement/logs"
mkdir -p "$LOG_DIR"

SUPERVISOR_LOG="$LOG_DIR/supervisor.log"

# Pass the requested iteration limit to run14.
export MAX_VERIFICATION_IMPROVEMENT_ITERATIONS="$MAX_ITERS"

# Force Python output to be emitted immediately.
# This makes run14's existing progress messages visible through tee
# instead of waiting for Python's stdout buffer to fill.
export PYTHONUNBUFFERED=1

log() {
    printf '%s\n' "$*" | tee -a "$SUPERVISOR_LOG"
}

log "=============================================================================="
log "ARGUS verification-improvement supervisor"
log "Project       : $PROJECT_ROOT"
log "Design config : $DESIGN_CONFIG"
log "Design        : $DESIGN_NAME"
log "Max iterations: $MAX_ITERS"
log "Log directory : $LOG_DIR"
log "Started       : $(date --iso-8601=seconds)"
log "=============================================================================="

# Allow Ctrl+C / SIGTERM to stop the supervisor cleanly.
STOP_REQUESTED=0

handle_stop() {
    STOP_REQUESTED=1
    log ""
    log "[SUPERVISOR] Stop requested by user."
}

trap handle_stop INT TERM

while true; do
    # If Ctrl+C was received between iterations, stop without starting
    # another run14 process.
    if [[ "$STOP_REQUESTED" -eq 1 ]]; then
        log "[SUPERVISOR] Stopping."
        exit 130
    fi

    RUN_TS="$(date '+%Y%m%d_%H%M%S')"
    RUN_LOG="$LOG_DIR/run_${RUN_TS}.log"

    {
        echo "=============================================================================="
        echo "Starting run14"
        echo "Time          : $(date --iso-8601=seconds)"
        echo "Project       : $PROJECT_ROOT"
        echo "Design config : $DESIGN_CONFIG"
        echo "Design        : $DESIGN_NAME"
        echo "Max iterations: $MAX_ITERS"
        echo "Run log       : $RUN_LOG"
        echo "=============================================================================="
    } | tee -a "$SUPERVISOR_LOG" "$RUN_LOG"

    # Run the normal pipeline module.
    #
    # -u guarantees unbuffered Python stdout/stderr.
    # tee simultaneously:
    #   1. shows run14 output in the terminal
    #   2. saves the exact output to RUN_LOG
    python -u -m pipeline.run14 \
        --design-config "$DESIGN_CONFIG" \
        2>&1 | tee -a "$RUN_LOG"

    status=${PIPESTATUS[0]}

    {
        echo "------------------------------------------------------------------------------"
        echo "run14 exit status: $status"
        echo "Time: $(date --iso-8601=seconds)"

        if [[ "$STOP_REQUESTED" -eq 1 ]]; then
            echo "Status: STOPPED BY USER"
            echo "Supervisor: stopping."

        elif [[ "$status" -eq 0 ]]; then
            echo "Status: CLEAN EXIT"
            echo "Supervisor: stopping."

        else
            echo "Status: PROCESS FAILURE"
            echo "Supervisor: restarting after ${RESTART_DELAY_SECONDS:-10}s."
        fi

        echo "------------------------------------------------------------------------------"
        echo
    } | tee -a "$SUPERVISOR_LOG" "$RUN_LOG"

    # User explicitly stopped the run.
    if [[ "$STOP_REQUESTED" -eq 1 ]]; then
        exit 130
    fi

    # Normal completion.
    if [[ "$status" -eq 0 ]]; then
        exit 0
    fi

    # Unexpected run14 failure.
    #
    # run14's persisted improvement_state.json and accepted TB snapshot
    # allow the next invocation to resume from the last accepted state.
    log "[SUPERVISOR] run14 failed."
    log "[SUPERVISOR] Restarting in ${RESTART_DELAY_SECONDS:-10}s..."
    sleep "${RESTART_DELAY_SECONDS:-10}"
done

