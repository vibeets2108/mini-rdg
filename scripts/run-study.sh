#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
run="study-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "results/$run"
monitor=""
cleanup() {
  if [[ -n "$monitor" ]]; then
    kill "$monitor" 2>/dev/null || true
    wait "$monitor" 2>/dev/null || true
  fi
  docker compose stop cassandra >/dev/null
}
trap cleanup EXIT
docker run --rm --cpus=2 --memory=2g \
  -v "$PWD:/work" -v mini-rdg-study-maven-cache:/root/.m2 -w /work \
  maven:3.9.9-eclipse-temurin-21 mvn -B -q -Daether.connector.requestTimeout=30000 clean verify
python3 -m unittest discover -s scripts -p 'test_*.py'
docker compose build bench
docker compose up -d --wait --wait-timeout 360 cassandra
docker compose run --rm bench seed
docker compose run --rm bench validate
docker compose config > "results/$run/compose-resolved.yaml"
docker compose images --format json > "results/$run/images.json"
docker image inspect cassandra:5.0.5 mini-rdg-study-bench \
  --format '{{json .RepoDigests}} {{.Id}}' > "results/$run/image-identities.txt"
(
  while true; do
    ids=$(docker ps -q --filter label=com.docker.compose.project=mini-rdg-study)
    if [[ -n "$ids" ]]; then
      printf '{"sample_time":"%s"}\n' "$(date -u +%FT%TZ)"
      # Only containers belonging to this Compose project are monitored.
      docker stats --no-stream --format '{{json .}}' $ids
    fi
    sleep 2
  done
) > "results/$run/container-stats.jsonl" &
monitor=$!
sha256sum target/concurrency-study-1.0-SNAPSHOT.jar 2>/dev/null \
  > "results/$run/local-jar.sha256" || true
docker compose run --rm bench study "--output=/results/$run" "$@"
python3 scripts/analyze.py "results/$run"
printf '\nCompleted study: results/%s\n' "$run"
