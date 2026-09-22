#!/bin/sh
# entrypoint.sh — chown bind-mounted state dirs to the non-root user,
# then drop privileges and exec the binary.
#
# Why: Docker bind-mounts created on first start are owned by root on
# the host (the image's chowned target dir is masked by the mount).
# Without this, `var/lib/conductor/device-id` write fails with EACCES
# in the typical home-lab "first start, no pre-created dir" case.

set -e

CONDUCTOR_USER="${CONDUCTOR_USER:-conductor}"
CONDUCTOR_UID="${CONDUCTOR_UID:-10001}"

# Ensure the data, DVR output, and self-hosted logo directories exist. Seed
# immutable built-in logos only when absent so operator replacements in the
# persistent bind mount are never overwritten on restart.
CONDUCTOR_LOGOS_DIR="${CONDUCTOR_LOGOS_DIR:-/var/lib/conductor/logos}"
export CONDUCTOR_LOGOS_DIR
mkdir -p /var/lib/conductor /var/lib/conductor/dvr "$CONDUCTOR_LOGOS_DIR"
if [ -d /usr/share/conductor/default-logos ]; then
    for logo in /usr/share/conductor/default-logos/*; do
        [ -f "$logo" ] || continue
        destination="$CONDUCTOR_LOGOS_DIR/$(basename "$logo")"
        if [ -L "$destination" ]; then
            echo "entrypoint: refusing symlinked logo destination: $destination" >&2
            exit 1
        fi
        if [ ! -e "$destination" ]; then
            cp "$logo" "$destination"
        elif [ ! -f "$destination" ]; then
            echo "entrypoint: refusing non-file logo destination: $destination" >&2
            exit 1
        fi
    done
fi
chown -R "$CONDUCTOR_UID:$CONDUCTOR_UID" /var/lib/conductor

# Drop privileges via whichever helper the base image ships with:
# alpine variant has su-exec; ubuntu/cuda variant has gosu.
if command -v gosu >/dev/null 2>&1; then
    exec gosu "$CONDUCTOR_USER" /usr/local/bin/conductor "$@"
elif command -v su-exec >/dev/null 2>&1; then
    exec su-exec "$CONDUCTOR_USER" /usr/local/bin/conductor "$@"
else
    echo "entrypoint: neither gosu nor su-exec available" >&2
    exit 1
fi
