# Default channel logos

Conductor serves a logo for a channel that has none of its own. The image
files in this directory are copied into the container at
`/usr/share/conductor/default-logos/` and imported on first start by
`scripts/entrypoint.sh`.

The published repository ships no logo files: sports and broadcaster marks
belong to their owners, not to this project. Supply your own — 96×96 PNG or
JPEG, named after the channel group it stands in for — and they will be
imported the same way. An empty directory is fine; the entrypoint skips the
import when there is nothing to import.
