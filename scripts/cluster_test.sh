#!/usr/bin/env bash
# cluster_test.sh — end-to-end cluster behaviour tests
#
# What this script tests:
#   1. All 3 nodes start and a leader is elected
#   2. A write on the leader replicates to all followers
#   3. A follower crash does NOT break the cluster (quorum = 2/3)
#   4. The dead follower restarts and catches up via log replay
#   5. The leader crashes; a new leader is elected and writes continue
#   6. The original leader restarts and catches up to the new leader
#
# Usage:
#   cd <project-root>
#   go build -o bin/kvstore .
#   go build -o bin/kvctl   ./cmd/kvctl
#   bash scripts/cluster_test.sh
#
# Requirements: bash, nc (netcat), sleep, kill

set -euo pipefail

# ── Paths ─────────────────────────────────────────────────────────────────────
BINARY="./bin/kvstore"
KVCTL="./bin/kvctl"
CONFIG="config/cluster.json"
DATA_DIR="data"

# ── Colour helpers ────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
pass() { echo -e "${GREEN}[PASS]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; FAILURES=$((FAILURES+1)); }
info() { echo -e "${YELLOW}[INFO]${NC} $*"; }
FAILURES=0

# ── Cleanup helpers ───────────────────────────────────────────────────────────
NODE1_PID="" NODE2_PID="" NODE3_PID=""

kill_node() {
    local pid=$1 name=$2
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        info "Killed $name (pid $pid)"
    fi
}

cleanup() {
    kill_node "$NODE1_PID" "node1"
    kill_node "$NODE2_PID" "node2"
    kill_node "$NODE3_PID" "node3"
    # Remove per-node WAL + Raft DB (snapshots live inside the .raft file).
    rm -f "$DATA_DIR"/node*.wal "$DATA_DIR"/node*.raft "$DATA_DIR"/node*.log 2>/dev/null || true
}
trap cleanup EXIT

# ── Pre-flight checks ─────────────────────────────────────────────────────────
if [[ ! -f "$BINARY" ]]; then
    echo "Binary not found. Run: go build -o bin/kvstore ."
    exit 1
fi
if [[ ! -f "$KVCTL" ]]; then
    echo "kvctl not found. Run: go build -o bin/kvctl ./cmd/kvctl"
    exit 1
fi
mkdir -p "$DATA_DIR" bin

# ── Start a node ──────────────────────────────────────────────────────────────
start_node() {
    local id=$1
    "$BINARY" --id "$id" --config "$CONFIG" --data "$DATA_DIR" \
        > "$DATA_DIR/${id}.log" 2>&1 &
    echo $!
}

# ── Wait for a node's KV port to accept connections ──────────────────────────
wait_for_port() {
    local port=$1 timeout=${2:-10}
    local i=0
    while ! nc -z localhost "$port" 2>/dev/null; do
        sleep 0.5; i=$((i+1))
        if [[ $i -ge $((timeout*2)) ]]; then
            fail "Port $port never opened (timeout ${timeout}s)"
            return 1
        fi
    done
}

# ── Send one KV command and return the response ───────────────────────────────
kv() {
    local port=$1 cmd=$2
    "$KVCTL" --server "localhost:$port" $cmd 2>/dev/null || echo "ERROR"
}

# ── Find which node is the leader (returns its KV port) ──────────────────────
find_leader_port() {
    for port in 8001 8002 8003; do
        local resp
        resp=$(kv "$port" "put __probe__ __val__" 2>/dev/null || echo "")
        if [[ "$resp" == "OK" ]]; then
            echo "$port"; return
        fi
    done
    echo ""
}

# ═════════════════════════════════════════════════════════════════════════════
echo ""
echo "══════════════════════════════════════════════════"
echo "  Distributed KV Store — Cluster Test Suite"
echo "══════════════════════════════════════════════════"
echo ""

# ── Test 1: Start all 3 nodes and wait for leader election ───────────────────
info "Test 1: Starting 3-node cluster and waiting for leader election..."

NODE1_PID=$(start_node node1)
NODE2_PID=$(start_node node2)
NODE3_PID=$(start_node node3)

info "  node1 pid=$NODE1_PID  node2 pid=$NODE2_PID  node3 pid=$NODE3_PID"

# Wait for all three KV ports to be ready.
wait_for_port 8001 15
wait_for_port 8002 15
wait_for_port 8003 15
info "  All 3 KV ports open. Waiting 2s for leader election..."
sleep 2

LEADER_PORT=$(find_leader_port)
if [[ -n "$LEADER_PORT" ]]; then
    pass "Leader elected — KV port $LEADER_PORT is accepting writes"
else
    fail "No leader elected after 2s"
fi

# ── Test 2: Write on leader, read back from all nodes ────────────────────────
info ""
info "Test 2: Write on leader, read from all 3 nodes..."

WRITE_RESP=$(kv "$LEADER_PORT" "put city London")
if [[ "$WRITE_RESP" == "OK" ]]; then
    pass "PUT city London → OK"
else
    fail "PUT failed: $WRITE_RESP"
fi

sleep 0.2  # give followers time to apply

ALL_OK=true
for port in 8001 8002 8003; do
    VAL=$(kv "$port" "get city")
    if [[ "$VAL" == "London" ]]; then
        pass "  GET city from :$port → London"
    else
        fail "  GET city from :$port → '$VAL' (expected London)"
        ALL_OK=false
    fi
done
$ALL_OK && pass "Write replicated to all 3 nodes"

# ── Test 3: Kill a follower — cluster should keep working ────────────────────
info ""
info "Test 3: Kill one follower — cluster should keep working with 2/3..."

# Find a follower (not the leader).
FOLLOWER_PORT="" FOLLOWER_PID=""
declare -A PORT_TO_PID=([8001]=$NODE1_PID [8002]=$NODE2_PID [8003]=$NODE3_PID)
for port in 8001 8002 8003; do
    if [[ "$port" != "$LEADER_PORT" ]]; then
        FOLLOWER_PORT=$port
        FOLLOWER_PID=${PORT_TO_PID[$port]}
        break
    fi
done

kill_node "$FOLLOWER_PID" "follower :$FOLLOWER_PORT"
sleep 0.5

WRITE_RESP=$(kv "$LEADER_PORT" "put country France")
if [[ "$WRITE_RESP" == "OK" ]]; then
    pass "PUT country France → OK (cluster still alive with 2/3 nodes)"
else
    fail "PUT failed after follower crash: $WRITE_RESP"
fi

# ── Test 4: Restart the dead follower and verify it catches up ───────────────
info ""
info "Test 4: Restart the dead follower and verify log catch-up..."

# Map the dead follower's port to its node ID.
declare -A PORT_TO_ID=([8001]="node1" [8002]="node2" [8003]="node3")
DEAD_ID=${PORT_TO_ID[$FOLLOWER_PORT]}

NEW_PID=$(start_node "$DEAD_ID")
# Update the right pid variable.
case $FOLLOWER_PORT in
    8001) NODE1_PID=$NEW_PID ;;
    8002) NODE2_PID=$NEW_PID ;;
    8003) NODE3_PID=$NEW_PID ;;
esac

wait_for_port "$FOLLOWER_PORT" 10
info "  $DEAD_ID restarted (pid $NEW_PID). Waiting 2s for catch-up..."
sleep 2

VAL=$(kv "$FOLLOWER_PORT" "get country")
if [[ "$VAL" == "France" ]]; then
    pass "Restarted node caught up — GET country → France"
else
    fail "Restarted node did NOT catch up — GET country → '$VAL' (expected France)"
fi

# ── Test 5: Kill the current leader — a new leader should be elected ─────────
info ""
info "Test 5: Kill the leader — new leader should be elected..."

declare -A PORT_TO_PID2=([8001]=$NODE1_PID [8002]=$NODE2_PID [8003]=$NODE3_PID)
OLD_LEADER_PID=${PORT_TO_PID2[$LEADER_PORT]}
OLD_LEADER_ID=${PORT_TO_ID[$LEADER_PORT]}

kill_node "$OLD_LEADER_PID" "leader :$LEADER_PORT"
# Zero out the pid so cleanup doesn't double-kill.
case $LEADER_PORT in
    8001) NODE1_PID="" ;;
    8002) NODE2_PID="" ;;
    8003) NODE3_PID="" ;;
esac

info "  Waiting 2s for new leader election..."
sleep 2

NEW_LEADER_PORT=$(find_leader_port)
if [[ -n "$NEW_LEADER_PORT" && "$NEW_LEADER_PORT" != "$LEADER_PORT" ]]; then
    pass "New leader elected on port $NEW_LEADER_PORT"
else
    fail "No new leader elected after leader crash"
    NEW_LEADER_PORT="${LEADER_PORT}"  # best-effort fallback
fi

WRITE_RESP=$(kv "$NEW_LEADER_PORT" "put status active")
if [[ "$WRITE_RESP" == "OK" ]]; then
    pass "Write after leader failover → OK"
else
    fail "Write after leader failover failed: $WRITE_RESP"
fi

# ── Test 6: Restart the old leader — it should catch up as follower ──────────
info ""
info "Test 6: Restart the old leader — should rejoin as follower and catch up..."

NEW_PID2=$(start_node "$OLD_LEADER_ID")
case $LEADER_PORT in
    8001) NODE1_PID=$NEW_PID2 ;;
    8002) NODE2_PID=$NEW_PID2 ;;
    8003) NODE3_PID=$NEW_PID2 ;;
esac

wait_for_port "$LEADER_PORT" 10
info "  Old leader $OLD_LEADER_ID restarted (pid $NEW_PID2). Waiting 2s..."
sleep 2

VAL=$(kv "$LEADER_PORT" "get status")
if [[ "$VAL" == "active" ]]; then
    pass "Old leader rejoined and caught up — GET status → active"
else
    fail "Old leader did NOT catch up — GET status → '$VAL' (expected active)"
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════"
if [[ $FAILURES -eq 0 ]]; then
    echo -e "${GREEN}All tests passed!${NC}"
else
    echo -e "${RED}$FAILURES test(s) failed.${NC}"
    echo "Check data/node*.log for details."
fi
echo "══════════════════════════════════════════════════"
echo ""

exit $FAILURES
