#!/bin/bash
# =============================================================================
# MEMORY GUARDIAN v2.0
# =============================================================================
# Monitors system memory and automatically pauses/resumes Slurm partition
# to prevent OOM kills that crash MongoDB and other services.
#
# STRATEGY (3-tier defense):
# 1. DRAIN: When free < PAUSE_THRESHOLD: Stop accepting new jobs
# 2. EMERGENCY KILL: When free < CRITICAL_THRESHOLD: Cancel oldest running job
# 3. RESUME: When free > RESUME_THRESHOLD: Accept new jobs again
#
# This reactive approach prevents system overload while allowing running jobs
# to complete in normal conditions, but kills jobs in emergencies.
# =============================================================================

# Configuration (can be overridden by environment variables)
# NOTE: Thresholds tuned for Docker Desktop ~16GB environments where memory-heavy jobs can spike quickly
#
# These values can be configured in environment-config.json under:
#   resource_requirements.memory_guardian.*
#
PAUSE_THRESHOLD_PCT=${MEMORY_GUARDIAN_PAUSE_PCT:-35}       # Drain when free < 35% (~5.6GB buffer)
CRITICAL_THRESHOLD_PCT=${MEMORY_GUARDIAN_CRITICAL_PCT:-15} # Emergency kill when free < 15% (~2.4GB)
RESUME_THRESHOLD_PCT=${MEMORY_GUARDIAN_RESUME_PCT:-50}     # Resume when free > 50% (~8GB buffer)
CHECK_INTERVAL=${MEMORY_GUARDIAN_INTERVAL:-3}              # Check every 3 seconds (fastest response)
KILL_COOLDOWN=${MEMORY_GUARDIAN_COOLDOWN:-30}              # Don't kill another job within N seconds
LOG_FILE="/var/log/memory-guardian.log"
PARTITION_NAME=${MEMORY_GUARDIAN_PARTITION:-debug}         # Partition to control

# State tracking
PAUSED=false
LAST_LOG_TIME=0
LAST_KILL_TIME=0
LOG_INTERVAL=60   # Only log status every 60s unless state changes

# Colors for console output
RED="\033[1;31m"
GREEN="\033[1;32m"
YELLOW="\033[1;33m"
CYAN="\033[1;36m"
NC="\033[0m"

log() {
    local level="$1"
    local message="$2"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [$level] $message" >> "$LOG_FILE"

    # Also echo to stdout for docker logs
    case "$level" in
        ERROR)   echo -e "${RED}[MEMORY-GUARDIAN] $message${NC}" ;;
        WARNING) echo -e "${YELLOW}[MEMORY-GUARDIAN] $message${NC}" ;;
        INFO)    echo -e "${GREEN}[MEMORY-GUARDIAN] $message${NC}" ;;
        DEBUG)   ;; # Don't echo debug to console
    esac
}

get_memory_stats() {
    # Read from /proc/meminfo (works in container)
    local mem_total=$(grep MemTotal /proc/meminfo | awk '{print $2}')
    local mem_available=$(grep MemAvailable /proc/meminfo | awk '{print $2}')

    # If MemAvailable not present (old kernel), calculate from Free+Cached+Buffers
    if [ -z "$mem_available" ]; then
        local mem_free=$(grep MemFree /proc/meminfo | awk '{print $2}')
        local mem_cached=$(grep "^Cached:" /proc/meminfo | awk '{print $2}')
        local mem_buffers=$(grep Buffers /proc/meminfo | awk '{print $2}')
        mem_available=$((mem_free + mem_cached + mem_buffers))
    fi

    # Calculate percentage
    local pct_free=$((mem_available * 100 / mem_total))

    # Return values via global variables
    MEM_TOTAL_MB=$((mem_total / 1024))
    MEM_AVAILABLE_MB=$((mem_available / 1024))
    MEM_FREE_PCT=$pct_free
}

pause_partition() {
    log "WARNING" "Memory critical (${MEM_FREE_PCT}% free < ${PAUSE_THRESHOLD_PCT}%). Draining node 'localhost'..."

    # DRAIN the NODE (not partition) - allows running jobs to complete but won't schedule new ones
    # In Slurm, DRAIN is a node state, not a partition state. Partitions can be UP/DOWN/INACTIVE.
    # DRAIN lets running jobs finish while blocking new job scheduling on this node.
    if scontrol update NodeName=localhost State=DRAIN Reason="MemoryGuardian: Low memory (${MEM_FREE_PCT}% free)" 2>/dev/null; then
        PAUSED=true
        log "WARNING" "Node 'localhost' set to DRAIN. Running jobs will complete, no new jobs will start."

        # Log current queue status
        local running=$(squeue -t RUNNING -h 2>/dev/null | wc -l)
        local pending=$(squeue -t PENDING -h 2>/dev/null | wc -l)
        log "INFO" "Queue status: $running running, $pending pending"

        # Log node state
        local node_state=$(scontrol show node=localhost 2>/dev/null | grep -oP 'State=\K[^ ]+' || echo "unknown")
        log "INFO" "Node state: $node_state"
    else
        log "ERROR" "Failed to drain node 'localhost'"
    fi
}

resume_partition() {
    log "INFO" "Memory recovered (${MEM_FREE_PCT}% free > ${RESUME_THRESHOLD_PCT}%). Resuming node 'localhost'..."

    # RESUME the node - this clears the DRAIN state and allows job scheduling again
    if scontrol update NodeName=localhost State=RESUME 2>/dev/null; then
        PAUSED=false
        log "INFO" "Node 'localhost' resumed. New jobs can now be scheduled."

        # Log current queue status
        local running=$(squeue -t RUNNING -h 2>/dev/null | wc -l)
        local pending=$(squeue -t PENDING -h 2>/dev/null | wc -l)
        log "INFO" "Queue status: $running running, $pending pending"

        # Log node state
        local node_state=$(scontrol show node=localhost 2>/dev/null | grep -oP 'State=\K[^ ]+' || echo "unknown")
        log "INFO" "Node state: $node_state"
    else
        log "ERROR" "Failed to resume node 'localhost'"
    fi
}

emergency_kill_job() {
    # Kill the oldest running job to free memory
    # This is a last resort when memory is critically low

    local current_time=$(date +%s)

    # Check cooldown to avoid killing too many jobs at once
    if [ $((current_time - LAST_KILL_TIME)) -lt $KILL_COOLDOWN ]; then
        log "WARNING" "Emergency kill skipped - cooldown active (${KILL_COOLDOWN}s)"
        return
    fi

    # Get oldest running job (sorted by start time)
    local oldest_job=$(squeue -t RUNNING -h -o "%i %S" 2>/dev/null | sort -k2 | head -1 | awk '{print $1}')

    if [ -n "$oldest_job" ]; then
        log "ERROR" "EMERGENCY: Memory critical (${MEM_FREE_PCT}% free < ${CRITICAL_THRESHOLD_PCT}%). Cancelling job $oldest_job..."

        if scancel "$oldest_job" 2>/dev/null; then
            LAST_KILL_TIME=$current_time
            log "ERROR" "Job $oldest_job cancelled to prevent OOM"

            # Log remaining queue status
            local running=$(squeue -t RUNNING -h 2>/dev/null | wc -l)
            local pending=$(squeue -t PENDING -h 2>/dev/null | wc -l)
            log "INFO" "Queue after kill: $running running, $pending pending"
        else
            log "ERROR" "Failed to cancel job $oldest_job"
        fi
    else
        log "WARNING" "No running jobs to kill"
    fi
}

check_and_act() {
    get_memory_stats

    local current_time=$(date +%s)
    local should_log=false

    # TIER 1: Check for critical memory - emergency kill
    if [ $MEM_FREE_PCT -lt $CRITICAL_THRESHOLD_PCT ]; then
        # First ensure we're paused
        if [ "$PAUSED" = false ]; then
            pause_partition
        fi
        # Then try to kill a job
        emergency_kill_job
        should_log=true
    # TIER 2: Check for low memory - pause/drain
    elif [ "$PAUSED" = false ] && [ $MEM_FREE_PCT -lt $PAUSE_THRESHOLD_PCT ]; then
        pause_partition
        should_log=true
    # TIER 3: Check for memory recovery - resume
    elif [ "$PAUSED" = true ] && [ $MEM_FREE_PCT -gt $RESUME_THRESHOLD_PCT ]; then
        resume_partition
        should_log=true
    fi

    # Periodic status logging (every LOG_INTERVAL seconds)
    if [ $((current_time - LAST_LOG_TIME)) -ge $LOG_INTERVAL ]; then
        local state_str="NORMAL"
        [ "$PAUSED" = true ] && state_str="PAUSED"
        log "DEBUG" "Status: ${MEM_AVAILABLE_MB}MB free (${MEM_FREE_PCT}%), partition=$state_str"
        LAST_LOG_TIME=$current_time
    fi
}

# Startup banner
log "INFO" "Memory Guardian v2.0 starting..."
log "INFO" "3-tier defense: DRAIN <${PAUSE_THRESHOLD_PCT}%, KILL <${CRITICAL_THRESHOLD_PCT}%, RESUME >${RESUME_THRESHOLD_PCT}%"
log "INFO" "Monitoring partition: $PARTITION_NAME, check interval: ${CHECK_INTERVAL}s, kill cooldown: ${KILL_COOLDOWN}s"

# Initial memory check
get_memory_stats
log "INFO" "Initial memory: ${MEM_AVAILABLE_MB}MB available of ${MEM_TOTAL_MB}MB (${MEM_FREE_PCT}% free)"

# Main loop
while true; do
    check_and_act
    sleep $CHECK_INTERVAL
done
