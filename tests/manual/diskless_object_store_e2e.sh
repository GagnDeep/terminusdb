#!/usr/bin/env bash
# End-to-end check of a TerminusDB deployment on a disk-less object store.
#
# Initializes a store, creates a database, inserts a schema and documents, and
# reads them back -- first with disk-less reads, then materialized against the
# same bucket. Both must return identical documents.
#
# Needs a MinIO (or any S3-compatible endpoint with conditional PUT). A local
# directory backend is deliberately not supported: object_store's LocalFileSystem
# has no ETag-conditional update, so the label compare-and-swap cannot work and
# the store would accept a first commit and then no more.
#
#   ./tests/manual/diskless_object_store_e2e.sh
#
# Override MINIO_ENDPOINT / BUCKET / TDB to point at an existing deployment.

set -euo pipefail

TDB="${TDB:-$(dirname "$0")/../../terminusdb}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://127.0.0.1:9377}"
BUCKET="${BUCKET:-tdbtest}"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
export AWS_ENDPOINT="$MINIO_ENDPOINT"
export AWS_ALLOW_HTTP=true
export AWS_REGION="${AWS_REGION:-us-east-1}"

export TERMINUSDB_OBJECT_STORE_BUCKET="$BUCKET"
export TERMINUSDB_OBJECT_STORE_PREFIX="${TERMINUSDB_OBJECT_STORE_PREFIX:-e2e/}"

DB="admin/diskless_e2e"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

cat > "$WORK/schema.json" <<'EOF'
{"@type":"@context","@base":"terminusdb:///data/","@schema":"terminusdb:///schema#"}
{"@type":"Class","@id":"Person","name":"xsd:string","age":"xsd:integer"}
EOF

echo "== initialize (disk-less) =="
export TERMINUSDB_DISKLESS_READS=true
"$TDB" store init
"$TDB" db create "$DB"
"$TDB" doc insert "$DB" -g schema -f < "$WORK/schema.json" > /dev/null
for n in alice bob carol; do
    echo "{\"@type\":\"Person\",\"name\":\"$n\",\"age\":30}"
done | "$TDB" doc insert "$DB" > /dev/null

echo "== read back, disk-less =="
"$TDB" doc get "$DB" | sort > "$WORK/diskless.txt"
cat "$WORK/diskless.txt"

echo "== read back, materialized, same bucket =="
unset TERMINUSDB_DISKLESS_READS
"$TDB" doc get "$DB" | sort > "$WORK/materialized.txt"

if ! diff -q "$WORK/diskless.txt" "$WORK/materialized.txt" > /dev/null; then
    echo "FAIL: disk-less and materialized reads differ"
    diff "$WORK/diskless.txt" "$WORK/materialized.txt" || true
    exit 1
fi

COUNT=$(wc -l < "$WORK/diskless.txt")
if [ "$COUNT" -ne 3 ]; then
    echo "FAIL: expected 3 documents, got $COUNT"
    exit 1
fi

echo "PASS: $COUNT documents, identical disk-less and materialized"
