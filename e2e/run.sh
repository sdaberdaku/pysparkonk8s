#!/usr/bin/env bash
# End-to-end test: Airflow 3 (KubernetesExecutor) on kind runs the three example DAGs, each of
# which starts a client-mode Spark session whose executors run as pods.
#
# Env: PYTHON_VERSION (3.10), EXECUTOR_IMAGE (spark:4.2.0-python3), CLUSTER (pysparkonk8s),
#      KEEP_CLUSTER=1 keeps the kind cluster after the run, ARTIFACT_DIR (e2e/out) receives
#      cluster dumps when the run fails.
set -euo pipefail

PYTHON_VERSION="${PYTHON_VERSION:-3.10}"
EXECUTOR_IMAGE="${EXECUTOR_IMAGE:-spark:4.2.0-python3}"
CLUSTER="${CLUSTER:-pysparkonk8s}"
KEEP_CLUSTER="${KEEP_CLUSTER:-}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARTIFACT_DIR="${ARTIFACT_DIR:-$ROOT/e2e/out}"
NS=airflow
API=http://localhost:8080
KUBECTL=(kubectl --context "kind-$CLUSTER" -n "$NS")
PF_PID=""
TOKEN=""

cleanup() {
  [[ -n "$PF_PID" ]] && kill "$PF_PID" 2>/dev/null || true
  if [[ -z "$KEEP_CLUSTER" && -z "${CI:-}" ]]; then
    kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

api() { # api METHOD PATH [JSON]
  curl -fsS -X "$1" "$API$2" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' ${3:+-d "$3"}
}

dump() {
  echo "::group::cluster dump" >&2
  mkdir -p "$ARTIFACT_DIR"
  kubectl --context "kind-$CLUSTER" get pods -A -o wide | tee "$ARTIFACT_DIR/pods.txt" >&2 || true
  kubectl --context "kind-$CLUSTER" get events -A --sort-by=.lastTimestamp >"$ARTIFACT_DIR/events.txt" 2>&1 || true
  "${KUBECTL[@]}" describe pods >"$ARTIFACT_DIR/describe-pods.txt" 2>&1 || true
  for pod in $("${KUBECTL[@]}" get pods -o name 2>/dev/null); do
    "${KUBECTL[@]}" logs "$pod" --all-containers --tail=-1 >"$ARTIFACT_DIR/${pod#pod/}.log" 2>&1 || true
  done
  echo "--- scheduler log (tail) ---" >&2
  "${KUBECTL[@]}" logs deploy/airflow-scheduler -c scheduler --tail=200 >&2 || true
  for f in "$ARTIFACT_DIR"/*.log; do
    case "$f" in *spark* | *driver* | *pysparkonk8s* | *sum-ids* | *scaled-sum*)
      echo "--- $f (tail) ---" >&2; tail -n 80 "$f" >&2 ;;
    esac
  done
  if [[ -n "${DAG_ID:-}" && -n "${RUN_ID:-}" && -n "$TOKEN" ]]; then
    local tis ti try
    tis=$(api GET "/api/v2/dags/$DAG_ID/dagRuns/$RUN_ID/taskInstances" 2>/dev/null | jq -c '.task_instances[]|{task_id,map_index,try_number,state}' || true)
    echo "$tis" >&2
    while read -r ti; do
      [[ -z "$ti" ]] && continue
      try=$(jq -r .try_number <<<"$ti")
      api GET "/api/v2/dags/$DAG_ID/dagRuns/$RUN_ID/taskInstances/$(jq -r .task_id <<<"$ti")/logs/$((try > 0 ? try : 1))?map_index=$(jq -r .map_index <<<"$ti")" \
        2>/dev/null | tee -a "$ARTIFACT_DIR/task-logs.json" | head -c 20000 >&2 || true
    done <<<"$tis"
  fi
  echo "::endgroup::" >&2
}

fail() {
  echo "FAIL: $*" >&2
  dump
  exit 1
}

echo "== build wheel and images (python $PYTHON_VERSION)"
cd "$ROOT"
rm -rf dist
uv build --wheel
docker build -f docker/driver.Dockerfile --build-arg "PYTHON_VERSION=$PYTHON_VERSION" -t pysparkonk8s-driver:base .
docker build -t pysparkonk8s-driver:e2e -f - . <<'DOCKERFILE'
FROM pysparkonk8s-driver:base
COPY examples/dags /opt/airflow/dags
DOCKERFILE
docker pull "$EXECUTOR_IMAGE"

echo "== kind cluster"
kind get clusters | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER" --wait 120s
# `kind load docker-image` fails with the containerd image store (multi-platform indexes and
# attestation manifests). Fall back to importing a single-platform archive straight into the node;
# ctr can exit non-zero on a stray manifest even though the image is imported, so check the result.
case "$(uname -m)" in aarch64 | arm64) PLATFORM=linux/arm64 ;; *) PLATFORM=linux/amd64 ;; esac
NODE="$CLUSTER-control-plane"
for image in pysparkonk8s-driver:e2e "$EXECUTOR_IMAGE"; do
  kind load docker-image --name "$CLUSTER" "$image" 2>/dev/null && continue
  docker save --platform "$PLATFORM" "$image" |
    docker exec --privileged -i "$NODE" ctr -n k8s.io images import --digests --snapshotter=overlayfs - >/dev/null 2>&1 || true
  docker exec "$NODE" crictl inspecti "$image" >/dev/null || { echo "could not load $image into kind" >&2; exit 1; }
done

echo "== install Airflow and RBAC"
# env[1] is EXECUTOR_IMAGE in e2e/values.yaml; the DAG file reads it in the dag processor and in the worker pod.
helm upgrade --install airflow apache-airflow/airflow --version 1.22.0 -n "$NS" --create-namespace \
  -f e2e/values.yaml \
  --set "env[1].value=$EXECUTOR_IMAGE" \
  --set "config.kubernetes_environment_variables.EXECUTOR_IMAGE=$EXECUTOR_IMAGE" \
  --wait --timeout 10m || { dump; exit 1; }
helm upgrade --install pysparkonk8s-rbac charts/pysparkonk8s-rbac -n "$NS" --set serviceAccountName=airflow-worker

echo "== port-forward and token"
"${KUBECTL[@]}" port-forward svc/airflow-api-server 8080:8080 >/dev/null 2>&1 &
PF_PID=$!
for _ in $(seq 60); do
  TOKEN=$(curl -fsS -X POST "$API/auth/token" -H 'Content-Type: application/json' \
    -d '{"username":"admin","password":"admin"}' 2>/dev/null | jq -r .access_token) && [[ -n "$TOKEN" && "$TOKEN" != null ]] && break
  TOKEN=""
  sleep 5
done
[[ -n "$TOKEN" ]] || fail "no API token"

executor_pods() { "${KUBECTL[@]}" get pods -l pysparkonk8s/component=executor --no-headers 2>/dev/null | wc -l; }

# expected XCom return values per DAG: task_id:map_index:value
declare -A EXPECT=(
  [pysparkonk8s_decorator]="sum_ids:-1:499500"
  [pysparkonk8s_helper]="sum_ids:-1:499500"
  [pysparkonk8s_mapped]="scaled_sum:0:499500 scaled_sum:1:999000"
)

for DAG_ID in pysparkonk8s_decorator pysparkonk8s_helper pysparkonk8s_mapped; do
  echo "== DAG $DAG_ID"
  # The dag processor needs a moment to parse the DAGs.
  for _ in $(seq 60); do
    api PATCH "/api/v2/dags/$DAG_ID" '{"is_paused": false}' >/dev/null 2>&1 && break
    sleep 5
  done
  RUN_ID=$(api POST "/api/v2/dags/$DAG_ID/dagRuns" '{"logical_date": null}' | jq -r .dag_run_id) || fail "trigger $DAG_ID"
  seen_executor=0
  state=""
  for _ in $(seq 180); do # 15 minutes
    state=$(api GET "/api/v2/dags/$DAG_ID/dagRuns/$RUN_ID" | jq -r .state)
    [[ $(executor_pods) -gt 0 ]] && seen_executor=1
    [[ "$state" == success || "$state" == failed ]] && break
    sleep 5
  done
  echo "run $RUN_ID: $state (executor pods seen: $seen_executor)"
  [[ "$state" == success ]] || fail "$DAG_ID run $RUN_ID ended in state '$state'"
  [[ $seen_executor -eq 1 ]] || fail "$DAG_ID never started an executor pod"
  for item in ${EXPECT[$DAG_ID]}; do
    IFS=: read -r task idx want <<<"$item"
    got=$(api GET "/api/v2/dags/$DAG_ID/dagRuns/$RUN_ID/taskInstances/$task/xcomEntries/return_value?map_index=$idx" | jq -r '.value|tostring')
    [[ "$got" == "$want" ]] || fail "$DAG_ID $task[$idx] XCom is '$got', expected $want"
    echo "XCom $task[$idx] = $got"
  done
done

echo "== executor pods are gone"
for _ in $(seq 24); do
  [[ $(executor_pods) -eq 0 ]] && break
  sleep 5
done
left=$(executor_pods)
[[ $left -eq 0 ]] || fail "$left executor pod(s) left behind"

echo "PASS"
