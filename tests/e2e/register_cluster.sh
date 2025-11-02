#!/bin/bash

output=$(docker compose --project-directory ../vendor/slurm-docker-cluster -f ../vendor/slurm-docker-cluster/docker-compose.yml -f docker-compose.override.yml --env-file .env.test exec slurmctld bash -c "/usr/bin/sacctmgr --immediate add cluster name=linux" 2>&1)
status=$?


if [ $status -ne 0 ] && echo "$output" | grep -q "cluster linux already exists"; then
    echo "Cluster already exists, ignoring error"
    status=0
    exit 0
fi

if [ $status -ne 0 ]; then
    echo "$output"
    exit $status
fi


docker compose --project-directory ../vendor/slurm-docker-cluster -f ../vendor/slurm-docker-cluster/docker-compose.yml -f docker-compose.override.yml --env-file .env.test restart slurmdbd slurmctld
