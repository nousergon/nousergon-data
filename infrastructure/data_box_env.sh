# shellcheck shell=bash
# Sourced on the data-spot box (infrastructure/lambdas/data-spot-dispatcher's
# bootstrap tail), from the checkout, with the venv active and BEFORE any
# workload runs. alpha-engine-config-I11203. It declares two things every run
# manifest the box writes reads from the process environment.
#
# 1. THE COMPUTE ROW. `nousergon_lib.run_manifest._resolve_compute` reads
#    NE_DATA_INSTANCE_TYPE / NE_DATA_LIFECYCLE and writes `local` when they are
#    unset. Nothing on this box set them, so every manifest it wrote said
#    `instance_type: local`: v1's and the shadow's 2026-09-23 D31 manifests both
#    did, and CloudTrail's RunInstances shows both boxes were c5.large spot.
#    They come from the instance metadata service, i.e. are measured on the box,
#    never passed in by the launcher. IMDS unreachable leaves them unset, which
#    the manifest then records as `local` — said loudly here, not guessed.
#
# 2. THE NUMERIC PIN, derived from features/numeric_pin.py, its only
#    definition — never restated here. Every Python entrypoint that computes
#    D31 applies it itself before numpy loads; exporting it shell-wide as well
#    means any other interpreter this box starts computes under the same one.

# The endpoint is overridable for tests only; on the box it is always the default.
_ne_imds="${NE_DATA_IMDS_ENDPOINT:-http://169.254.169.254}"
_ne_imds_token=$(curl -fsS --noproxy "*" --max-time 2 -X PUT \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 300" \
    "${_ne_imds}/latest/api/token" 2>/dev/null) || _ne_imds_token=""
if [ -n "$_ne_imds_token" ]; then
    _ne_type=$(curl -fsS --noproxy "*" --max-time 2 -H "X-aws-ec2-metadata-token: ${_ne_imds_token}" \
        "${_ne_imds}/latest/meta-data/instance-type" 2>/dev/null) || _ne_type=""
    _ne_life=$(curl -fsS --noproxy "*" --max-time 2 -H "X-aws-ec2-metadata-token: ${_ne_imds_token}" \
        "${_ne_imds}/latest/meta-data/instance-life-cycle" 2>/dev/null) || _ne_life=""
    [ -n "$_ne_type" ] && export NE_DATA_INSTANCE_TYPE="$_ne_type"
    [ -n "$_ne_life" ] && export NE_DATA_LIFECYCLE="$_ne_life"
fi
if [ -z "${NE_DATA_INSTANCE_TYPE:-}" ]; then
    echo "[data-box-env] WARNING: instance metadata unreachable — run manifests will record compute.instance_type=local"
else
    echo "[data-box-env] compute row: instance_type=${NE_DATA_INSTANCE_TYPE} lifecycle=${NE_DATA_LIFECYCLE:-unknown}"
fi
unset _ne_imds _ne_imds_token _ne_type _ne_life

_ne_pin=$(python -m features.numeric_pin --shell-exports) || {
    echo "[data-box-env] FATAL: could not derive the numeric pin (python -m features.numeric_pin)"
    return 1
}
eval "$_ne_pin"
echo "[data-box-env] numeric pin: $(printf '%s\n' "$_ne_pin" | sed -n 's/^export //p' | tr '\n' ' ')"
unset _ne_pin
