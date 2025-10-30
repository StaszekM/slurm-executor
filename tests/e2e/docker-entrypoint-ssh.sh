#!/bin/bash
set -e

# Start SSH daemon
/usr/sbin/sshd

# Execute the original entrypoint with the provided command
exec /usr/local/bin/docker-entrypoint.sh "$@"
