#!/usr/bin/env bash
# Provision a CodeBuild-managed ephemeral GitHub Actions runner for one repo.
#
# Replaces the bespoke webhook->Lambda->EC2-spot->JIT-runner->reaper subsystem
# (the *-runner-dispatcher Lambdas + their AMIs/bootstrap) with AWS-managed
# CodeBuild runners: arm64/Graviton, per-second-ish billing, no host to boot,
# no AMI to bake, no reaper to own. See cost-I2864.
#
# Idempotent: safe to re-run; updates the project/webhook/role in place.
#
# Prereqs (one-time, account-wide, NOT created here):
#   - A CodeConnections GitHub App connection (AVAILABLE) authorized on the
#     nousergon org with access to the target repos. Pass its ARN as CONNECTION_ARN
#     or set the default below. Created via:
#       aws codeconnections create-connection --provider-type GitHub --connection-name nousergon-codebuild
#     then completed in the console (browser authorize + install the app on the org).
#
# Usage:
#   ./deploy_codebuild_runner.sh <repo-short-name>
#   e.g. ./deploy_codebuild_runner.sh telos      # -> project telos-runner on nousergon/telos
#
# After running, re-point the repo's workflow job:
#   runs-on: codebuild-<repo>-runner-${{ github.run_id }}-${{ github.run_attempt }}
set -euo pipefail

REPO_SHORT="${1:?usage: deploy_codebuild_runner.sh <repo-short-name> (e.g. telos)}"
ORG="${GH_ORG:-nousergon}"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT="${AWS_ACCOUNT_ID:-711398986525}"
CONNECTION_ARN="${CONNECTION_ARN:-arn:aws:codeconnections:us-east-1:711398986525:connection/d83f3748-fa06-476c-9806-609df2d87e34}"
# arm64/Graviton EC2 compute, smallest size. Curated ARM standard image (has
# git/curl/tar/docker). Per-job override still possible via the runs-on label.
IMAGE="${CODEBUILD_IMAGE:-aws/codebuild/amazonlinux-aarch64-standard:4.0}"
COMPUTE="${CODEBUILD_COMPUTE:-BUILD_GENERAL1_SMALL}"

PROJECT="${REPO_SHORT}-runner"
ROLE="codebuild-${REPO_SHORT}-runner-role"
LOG_GROUP="/aws/codebuild/${PROJECT}"
REPO_URL="https://github.com/${ORG}/${REPO_SHORT}.git"

echo "[codebuild-runner] repo=${ORG}/${REPO_SHORT} project=${PROJECT} image=${IMAGE} compute=${COMPUTE}"

# ── 1. Service role (logs + use the shared GitHub connection ONLY). Extend this
#       per-repo if that repo's jobs need AWS access (telos gitleaks needs none).
TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"codebuild.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
PERMS=$(cat <<JSON
{"Version":"2012-10-17","Statement":[
 {"Sid":"Logs","Effect":"Allow","Action":["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"],"Resource":["arn:aws:logs:${REGION}:${ACCOUNT}:log-group:${LOG_GROUP}","arn:aws:logs:${REGION}:${ACCOUNT}:log-group:${LOG_GROUP}:*"]},
 {"Sid":"UseGitHubConnection","Effect":"Allow","Action":["codeconnections:GetConnectionToken","codeconnections:GetConnection","codeconnections:UseConnection","codestar-connections:UseConnection"],"Resource":"${CONNECTION_ARN}"}
]}
JSON
)
if ! aws iam get-role --role-name "$ROLE" >/dev/null 2>&1; then
  aws iam create-role --role-name "$ROLE" --assume-role-policy-document "$TRUST" \
    --description "CodeBuild GitHub Actions runner for ${ORG}/${REPO_SHORT} - cost-I2864" >/dev/null
  echo "[codebuild-runner] created role ${ROLE}"
fi
aws iam put-role-policy --role-name "$ROLE" --policy-name "${REPO_SHORT}-runner-inline" --policy-document "$PERMS"
ROLE_ARN="arn:aws:iam::${ACCOUNT}:role/${ROLE}"

# ── 2. Runner project (buildspec empty = CodeBuild injects the runner setup).
PROJECT_JSON=$(cat <<JSON
{
  "name": "${PROJECT}",
  "description": "Ephemeral GitHub Actions runner for ${ORG}/${REPO_SHORT} (cost-I2864 CodeBuild migration)",
  "source": {"type": "GITHUB", "location": "${REPO_URL}", "buildspec": "", "auth": {"type": "CODECONNECTIONS", "resource": "${CONNECTION_ARN}"}},
  "artifacts": {"type": "NO_ARTIFACTS"},
  "environment": {"type": "ARM_CONTAINER", "image": "${IMAGE}", "computeType": "${COMPUTE}", "imagePullCredentialsType": "CODEBUILD"},
  "serviceRole": "${ROLE_ARN}",
  "logsConfig": {"cloudWatchLogs": {"status": "ENABLED", "groupName": "${LOG_GROUP}"}}
}
JSON
)
if aws codebuild batch-get-projects --names "$PROJECT" --query 'projects[0].name' --output text 2>/dev/null | grep -q "$PROJECT"; then
  echo "$PROJECT_JSON" | aws codebuild update-project --cli-input-json file:///dev/stdin >/dev/null
  echo "[codebuild-runner] updated project ${PROJECT}"
else
  echo "$PROJECT_JSON" | aws codebuild create-project --cli-input-json file:///dev/stdin >/dev/null
  echo "[codebuild-runner] created project ${PROJECT}"
fi

# ── 3. Webhook: trigger a build (ephemeral runner) on each queued workflow job.
if ! aws codebuild batch-get-projects --names "$PROJECT" --query 'projects[0].webhook.url' --output text 2>/dev/null | grep -q 'http'; then
  aws codebuild create-webhook --project-name "$PROJECT" \
    --filter-groups '[[{"type":"EVENT","pattern":"WORKFLOW_JOB_QUEUED"}]]' >/dev/null
  echo "[codebuild-runner] created WORKFLOW_JOB_QUEUED webhook"
else
  echo "[codebuild-runner] webhook already present"
fi

echo "[codebuild-runner] DONE. Set the job's runs-on to:"
echo "  codebuild-${PROJECT}-\${{ github.run_id }}-\${{ github.run_attempt }}"
