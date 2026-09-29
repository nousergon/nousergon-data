#!/usr/bin/env bash
# package_data_gate.sh — ONE definition of the data_gate files a Lambda zip must
# carry to run the shared run-manifest predicate, sourced by every deploy.sh that
# packages it (data-spot-dispatcher, collection-readiness-probe).
#
# WHY (alpha-engine-config-I11269, measured 2026-09-29): both deploy scripts
# hand-copied the loader's three .py files and registry.d/units/, but
# descriptors.py also reads data_gate/config/consumer_repos.yaml (added by
# nousergon-data#1901 on 2026-09-24). Neither zip carried it, so every
# completion check raised FileNotFoundError: ne-data-collection-morning failed
# at VerifyRunManifests, and the first post-cutover preopen exhausted
# WaitForCollectionManifests on 12 raising polls and ended DegradedRun. Two
# hand-kept copy lists is how one of them misses a file; this is the one list,
# and tests/test_data_gate_lambda_package.py loads the units from a package
# built by it, so a new runtime read fails in review, not at 07:30 ET.
#
# Usage (from a deploy.sh that defines SCRIPT_DIR):
#   source "${SCRIPT_DIR}/../_shared/package_data_gate.sh"
#   package_data_gate "${PKG}" "${REPO_ROOT_DIR}"
#
# Files land at their repo-relative paths: descriptors.py resolves every path
# from its own location, so the zip root stands in for the repo root.

DATA_GATE_RUNTIME_FILES=(
  "data_gate/__init__.py"
  "data_gate/descriptors.py"
  "data_gate/run_manifest_predicate.py"
  "data_gate/config/consumer_repos.yaml"
)

package_data_gate() {
  local pkg="$1" repo_root="$2" rel
  mkdir -p "${pkg}/registry.d/units"
  for rel in "${DATA_GATE_RUNTIME_FILES[@]}"; do
    mkdir -p "${pkg}/$(dirname "${rel}")"
    cp "${repo_root}/${rel}" "${pkg}/${rel}"
  done
  cp "${repo_root}"/registry.d/units/*.yaml "${pkg}/registry.d/units/"
  echo "Packaged $(ls "${pkg}/registry.d/units" | wc -l | tr -d ' ') unit descriptors + the data_gate runtime (${#DATA_GATE_RUNTIME_FILES[@]} files)"
}
