#!/bin/bash

output=$(docker compose -f docker-compose.test.yml exec slurmctld bash -c "/usr/bin/sacctmgr --immediate add cluster name=linux" 2>&1)
status=$?


if [ $status -ne 0 ] && echo "$output" | grep -q "cluster linux already exists"; then
    echo "Cluster already exists, ignoring error"
    status=0
fi

if [ $status -ne 0 ]; then
    echo "$output"
    exit $status
fi


docker compose -f docker-compose.test.yml restart slurmdbd slurmctld
