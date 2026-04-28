#!/usr/bin/env bash
# Daily cron: convert yesterday's closed day-file from bz2 to lz4.
#
# Run this once after bulk_convert.sh completes, and keep it scheduled
# until the cutover.  Schedule via crontab:
#
#   0 6 * * * /path/to/scripts/tail_convert.sh /data2/feder/main /data2/feder/temp_lz4
#
# The script converts the file for UTC yesterday (the day most recently
# closed by the ingester).  It also converts the day before and two days
# ahead to handle the ingester's 2-3 day lag window, skipping files that
# have already been converted.

set -euo pipefail

SRC_DIR="${1:?Usage: $0 <src_dir> <dst_dir>}"
DST_DIR="${2:?Usage: $0 <src_dir> <dst_dir>}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

convert_day() {
    local offset="$1"
    local date_str
    date_str=$(date -u -d "${offset} days" +%Y/%-j 2>/dev/null || \
               python3 -c "
from datetime import date, timedelta
d = date.today() + timedelta(days=${offset})
print(f'{d.year}/{d.timetuple().tm_yday}')
")
    local year="${date_str%%/*}"
    local doy="${date_str##*/}"
    local doy_padded
    doy_padded=$(printf '%03d' "$doy")

    local src="$SRC_DIR/$year/$year-$doy_padded.sqlite"
    local dst="$DST_DIR/$year/$year-$doy_padded.sqlite"

    [ -f "$src" ] || return 0   # file doesn't exist yet, skip
    [ -f "$dst" ] && return 0   # already converted, skip

    mkdir -p "$(dirname "$dst")"
    uv run python "$SCRIPT_DIR/convert_file.py" "$src" "$dst"
}

# Convert yesterday plus the ingester's typical lag window.
for offset in -2 -1 0 1 2; do
    convert_day "$offset"
done
