#!/usr/bin/env bash
# Slurm job array: convert all Feder day-files from bz2 to lz4.
#
# Usage:
#   sbatch scripts/bulk_convert.sh <src_dir> <dst_dir>
#
# Runs up to 20 tasks concurrently, one file per task.  Failed tasks are
# marked FAILED in the Slurm log; re-run them with:
#   sbatch --array=<failed_indices> scripts/bulk_convert.sh <src_dir> <dst_dir>
#
# After all tasks complete, run verify_counts.py as a final sweep.

#SBATCH --job-name=feder-bz2-to-lz4
#SBATCH --output=logs/convert_%A_%a.out
#SBATCH --error=logs/convert_%A_%a.err
#SBATCH --time=02:00:00
#SBATCH --mem=4G
#SBATCH --cpus-per-task=1

set -euo pipefail

SRC_DIR="${1:?Usage: $0 <src_dir> <dst_dir>}"
DST_DIR="${2:?Usage: $0 <src_dir> <dst_dir>}"

# Build sorted list of all source files.
mapfile -t FILES < <(find "$SRC_DIR" -name '*.sqlite' | sort)
TOTAL=${#FILES[@]}

if [ -z "${SLURM_ARRAY_TASK_ID:-}" ]; then
    # Not running under Slurm: submit as a job array.
    mkdir -p logs
    sbatch --array="0-$((TOTAL - 1))%20" "$0" "$SRC_DIR" "$DST_DIR"
    echo "Submitted array of $TOTAL tasks (max 20 concurrent)."
    exit 0
fi

IDX=$SLURM_ARRAY_TASK_ID
if [ "$IDX" -ge "$TOTAL" ]; then
    echo "Index $IDX out of range ($TOTAL files)" >&2
    exit 1
fi

SRC="${FILES[$IDX]}"
# Reconstruct destination path with same relative structure.
REL="${SRC#"$SRC_DIR"/}"
DST="$DST_DIR/$REL"
mkdir -p "$(dirname "$DST")"

uv run python scripts/convert_file.py "$SRC" "$DST"
