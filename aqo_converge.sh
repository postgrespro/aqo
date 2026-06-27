#!/bin/bash
# Train AQO on the JOB benchmark PER QUERY until its cardinality-error series
# converges, or until a hard cap of runs is reached (default 100).
#
# "Converged" uses exactly AQO's own criterion (auto_tuning.c:converged_cq):
#   - need at least window+2 samples (default window = 5  =>  7 runs)
#   - take the last window+1 (=6) samples; let est = mean of the first 5 and
#     last = the 6th; the series is stable (converged) iff `last` is within an
#     absolute OR relative epsilon (default 0.01) of `est` on both sides.
# The series read is `cardinality_error_with_aqo` from aqo_query_stat — the
# history of the TOTAL cardinality error (avg over plan nodes) per execution,
# i.e. the gap between AQO's row predictions and the actual row counts.
#
# For every query the script reports:
#   converged          yes/no  -- did the error series converge within the cap
#   conv_pct           %       -- error reduction: (err_first-err_last)/err_first*100
#   card_error_total   double  -- final total cardinality error (err_last)
#   card_error_first   double  -- baseline error on the first run (err_first)
#   runs               int     -- how many executions were actually performed
#
# AQO learns from the previous execution: run 1 plans with PG's stock estimates
# and feeds back the observed row counts; run 2 uses the corrected estimates,
# and so on, until the error stops changing.
#
# Prerequisites:
#   - PG built with the AQO core hooks patch (contrib/aqo/aqo_master.patch)
#   - shared_preload_libraries = 'aqo'  (see aqo_setup.sh)
#   - CREATE EXTENSION aqo in the target database
#   - the JOB 'imdb' database loaded (schema.sql + copy.sql + fkindexes.sql)
#
# Usage:
#   ./aqo_converge.sh [database] [max_runs] [--mode=learn|forced|intelligent]
#                     [--no-reset] [--queries=path] [--statement-timeout=ms]
#
# Defaults: database=imdb, max_runs=100, mode=learn, reset AQO state first.
#
# Override the cluster location for your VM, e.g.:
#   PG_BASE=$HOME/postgres_master PGDATA=$HOME/postgres_master/vacuum_stats9 \
#   PGPORT=5499 ./aqo_converge.sh imdb 100
#
# Outputs (RESULTS_DIR/aqo_converge/<run-id>/):
#   convergence_report.csv  -- query,queryid,converged,conv_pct,card_error_total,
#                              card_error_first,runs,note
#   log.txt                 -- per-query progress

set -uo pipefail            # NOT -e: per-query failures are handled, not fatal
cd "$(dirname "$0")"
source ./lib.sh

# ---- arg parsing --------------------------------------------------------
DB="imdb"
MAX_RUNS_LOCAL="${MAX_RUNS:-100}"
MODE="learn"
DO_RESET=1
QUERIES_DIR="$QUERY_FILES"
STMT_TIMEOUT="${STATEMENT_TIMEOUT_MS:-600000}"

# Convergence criterion knobs — keep in sync with the server's GUCs
# (aqo.auto_tuning_window_size, auto_tuning_convergence_error).
WIN="${AQO_WINDOW:-5}"
EPS="${AQO_CONV_EPS:-0.01}"
# JOB queries all have several joins; keep AQO's default join threshold (3),
# but allow forcing 0 so every query is learned regardless of join count.
JOIN_THRESHOLD="${AQO_JOIN_THRESHOLD:-3}"

POSITIONAL=()
for arg in "$@"; do
    case "$arg" in
        --mode=*)              MODE="${arg#--mode=}" ;;
        --no-reset)            DO_RESET=0 ;;
        --queries=*)           QUERIES_DIR="${arg#--queries=}" ;;
        --statement-timeout=*) STMT_TIMEOUT="${arg#--statement-timeout=}" ;;
        --*)                   echo "Unknown flag: $arg" >&2; exit 2 ;;
        *)                     POSITIONAL+=("$arg") ;;
    esac
done
[[ ${#POSITIONAL[@]} -ge 1 ]] && DB="${POSITIONAL[0]}"
[[ ${#POSITIONAL[@]} -ge 2 ]] && MAX_RUNS_LOCAL="${POSITIONAL[1]}"

case "$MODE" in
    learn|forced|intelligent) ;;
    *) echo "mode must be learn|forced|intelligent, got '$MODE'" >&2; exit 2 ;;
esac

export PGDATABASE="$DB"
# -tA: tuples-only, unaligned — easy to read scalar/array results in bash.
PSQL_Q="$INSTDIR/psql -p $PGPORT -d $DB -U $PGUSER -X -q -tA"

# ---- output paths -------------------------------------------------------
RUN_ID="$(date +%Y%m%d_%H%M%S)"
OUT_DIR="$RESULTS_DIR/aqo_converge/$RUN_ID"
mkdir -p "$OUT_DIR"
REPORT="$OUT_DIR/convergence_report.csv"
LOG="$OUT_DIR/log.txt"

# ---- preflight ----------------------------------------------------------
pg_ensure_up

have_aqo=$($PSQL_Q -c "SELECT 1 FROM pg_extension WHERE extname='aqo'" 2>/dev/null || echo "")
if [[ -z "$have_aqo" ]]; then
    echo "FATAL: aqo extension not installed in database $DB" >&2
    echo "Hint: run ./aqo_setup.sh $DB   (needs shared_preload_libraries=aqo)" >&2
    exit 1
fi

queries=( "$QUERIES_DIR"/*.sql )
total_queries=${#queries[@]}
if [[ "$total_queries" -eq 0 ]]; then
    echo "FATAL: no queries found in $QUERIES_DIR" >&2; exit 1
fi

{
    echo "RUN_ID=$RUN_ID"
    echo "DB=$DB  MAX_RUNS=$MAX_RUNS_LOCAL  MODE=$MODE  RESET=$DO_RESET"
    echo "QUERIES_DIR=$QUERIES_DIR  count=$total_queries"
    echo "CONVERGENCE: window=$WIN  eps=$EPS  join_threshold=$JOIN_THRESHOLD"
    echo "STMT_TIMEOUT_MS=$STMT_TIMEOUT"
    echo "Output: $OUT_DIR"
} | tee "$LOG"

if [[ "$DO_RESET" -eq 1 ]]; then
    echo "Resetting AQO state via aqo_reset()..." | tee -a "$LOG"
    $PSQL_Q -c "SELECT count(*) FROM aqo_reset();" >>"$LOG" 2>&1 || true
fi

echo "query,queryid,converged,conv_pct,card_error_total,card_error_first,runs,note" > "$REPORT"

# ---- helpers ------------------------------------------------------------

# GUCs applied to every session. force_collect_stat guarantees the error
# series is recorded; show_hash makes EXPLAIN print the AQO "Query hash"
# (== queryid key in aqo_query_stat), which we use to find this query's row.
gucs_block() {
    cat <<EOF
LOAD 'aqo';
SET aqo.mode = '$MODE';
SET aqo.force_collect_stat = on;
SET aqo.show_details = on;
SET aqo.show_hash = on;
SET aqo.join_threshold = $JOIN_THRESHOLD;
SET statement_timeout = ${STMT_TIMEOUT};
EOF
}

# Run one query once under EXPLAIN ANALYZE (executes => AQO learns).
# Echoes the EXPLAIN text on success; empty string on timeout/error.
run_once() {
    local qf="$1"
    "$INSTDIR/psql" -p "$PGPORT" -d "$DB" -U "$PGUSER" -X -f /dev/stdin 2>/dev/null <<EOF
$(gucs_block)
EXPLAIN (ANALYZE, TIMING OFF, SUMMARY OFF, COSTS OFF, BUFFERS OFF)
$(sed 's/;[[:space:]]*$//' "$qf");
EOF
}

# Apply AQO's converged_cq() to a space-separated error series on stdin.
# Prints: "<conv 0|1> <first> <last> <n>"
analyze_series() {
    awk -v win="$WIN" -v eps="$EPS" '
    {
        n = NF
        if (n == 0) { print "0 NA NA 0"; next }
        for (i = 1; i <= n; i++) v[i] = $i + 0
        first = v[1]; last = v[n]; conv = 0
        if (n >= win + 2) {
            # last win+1 samples are v[n-win .. n]; est = mean of first `win`,
            # compared against the newest sample v[n].
            s = 0
            for (i = n - win; i <= n - 1; i++) s += v[i]
            est = s / win
            lst = v[n]
            up = (est * (1 + eps) > lst) || (est + eps > lst)
            lo = (est * (1 - eps) < lst) || (est - eps < lst)
            if (up && lo) conv = 1
        }
        printf "%d %s %s %d\n", conv, first, last, n
    }'
}

# ---- main loop ----------------------------------------------------------
n_converged=0
n_done=0
qi=0
for qf in "${queries[@]}"; do
    qi=$((qi + 1))
    name="$(basename "$qf" .sql)"
    qid=""
    runs=0
    conv=0; first="NA"; last="NA"
    note=""

    for ((r = 1; r <= MAX_RUNS_LOCAL; r++)); do
        out="$(run_once "$qf")"
        if [[ -z "$out" ]]; then
            note="timeout_or_error_at_run_${r}"
            break
        fi
        runs=$r

        # Resolve this query's AQO queryid once (stable across runs).
        if [[ -z "$qid" ]]; then
            qid="$(printf '%s\n' "$out" | awk -F': ' '/Query hash:/ {gsub(/[^0-9-]/,"",$2); print $2; exit}')"
            if [[ -z "$qid" ]]; then
                note="no_query_hash(aqo_disabled_for_query?)"
                break
            fi
        fi

        series="$($PSQL_Q -c "SELECT array_to_string(cardinality_error_with_aqo, ' ') FROM aqo_query_stat WHERE queryid = $qid" 2>/dev/null)"
        read -r conv first last _n <<<"$(printf '%s\n' "$series" | analyze_series)"

        if [[ "$conv" == "1" ]]; then
            break
        fi
    done

    # conv_pct = error reduction from the first run to the last.
    pct="$(awk -v f="$first" -v l="$last" 'BEGIN {
        if (f == "NA" || f + 0 <= 0) { print (l != "NA" && l + 0 <= 0) ? "100.0" : "NA"; exit }
        p = (f - l) / f * 100.0
        if (p < 0) p = 0
        printf "%.1f", p
    }')"
    convtxt="no"; [[ "$conv" == "1" ]] && { convtxt="yes"; n_converged=$((n_converged + 1)); }
    [[ "$runs" -gt 0 ]] && n_done=$((n_done + 1))

    echo "$name,$qid,$convtxt,$pct,$last,$first,$runs,$note" >> "$REPORT"
    printf "[%s] %3d/%3d  %-6s converged=%-3s conv%%=%-6s err=%-10s runs=%-3s %s\n" \
        "$(date +%H:%M:%S)" "$qi" "$total_queries" "$name" "$convtxt" "$pct" "$last" "$runs" "$note" \
        | tee -a "$LOG"
done

# ---- summary ------------------------------------------------------------
{
    echo ""
    echo "=== AQO convergence training done ==="
    echo "  queries trained : $n_done / $total_queries"
    echo "  converged       : $n_converged / $total_queries"
    echo "  report          : $REPORT"
    echo "  log             : $LOG"
    echo ""
    echo "Per-query result (sorted by convergence %):"
    printf "  %-7s %-9s %-8s %-12s %-5s\n" "query" "converged" "conv_%" "card_error" "runs"
    awk -F, 'NR>1 {printf "  %-7s %-9s %-8s %-12s %-5s\n", $1,$3,$4,$5,$7}' "$REPORT" \
        | sort -k3 -t' ' -nr 2>/dev/null || \
        awk -F, 'NR>1 {printf "  %-7s %-9s %-8s %-12s %-5s\n", $1,$3,$4,$5,$7}' "$REPORT"
} | tee -a "$LOG"
