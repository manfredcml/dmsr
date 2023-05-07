#!/bin/sh

set -e

# Start the Docker container
docker-compose -f tests/docker-compose.yml up -d

# Ensure that docker-compose down is executed even if cargo test fails
trap "docker-compose -f tests/docker-compose.yml down" EXIT

# Run the tests
cargo test -- --test-threads=1 --show-output
