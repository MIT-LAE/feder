#!/usr/bin/env bash
# Local bulk conversion using GNU Parallel: convert all Feder day-files
# from bz2 to lz4.
#
# Usage:
#   scripts/bulk_convert_parallel.sh <src_dir> <dst_dir> [--jobs N]
#
# Options:
#   --jobs N   Number of parallel workers (default: number of CPU cores)
#
# A job log is written to logs/parallel_convert.log.  Failed files are
# listed at the end; re-run them with:
#   parallel --joblog logs/parallel_convert.log --retry-failed
#
# After all files are done, run verify_counts.py as a final sweep.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! command -v parallel &>/dev/null; then
    echo "Error: GNU Parallel is not installed." >&2
    echo "  Install with: sudo apt install parallel  OR  brew install parallel" >&2
    exit 1
fi

SRC_DIR="${1:?Usage: $0 <src_dir> <dst_dir> [--jobs N]}"
DST_DIR="${2:?Usage: $0 <src_dir> <dst_dir> [--jobs N]}"
shift 2
JOBS="${JOBS:-$(nproc)}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --jobs) JOBS="$2"; shift 2 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

mkdir -p logs

# Build a tab-separated list of src<TAB>dst pairs and pipe to parallel.
# parallel receives {1}=src and {2}=dst via --colsep.
find "$SRC_DIR" -name '*.sqlite' | sort | \
    awk -v src="$SRC_DIR" -v dst="$DST_DIR" \
        '{ rel=substr($0, length(src)+2); print $0 "\t" dst "/" rel }' | \
    parallel \
        --jobs "$JOBS" \
        --colsep '\t' \
        --bar \
        --joblog logs/parallel_convert.log \
        --halt soon,fail=1 \
        'mkdir -p "$(dirname {2})" && uv run python '"$SCRIPT_DIR"'/convert_file.py {1} {2}'

# Summarise failures from the job log (exit value column is 7th field).
FAILED=$(awk 'NR>1 && $7 != 0' logs/parallel_convert.log | wc -l)
TOTAL=$(awk 'NR>1' logs/parallel_convert.log | wc -l)

echo ""
echo "Converted $((TOTAL - FAILED))/$TOTAL files successfully."
if [ "$FAILED" -gt 0 ]; then
    echo "$FAILED file(s) failed.  Re-run failures with:" >&2
    echo "  parallel --joblog logs/parallel_convert.log --retry-failed" >&2
    exit 1
fi
