FROM --platform=linux/amd64 public.ecr.aws/lambda/python:3.12

# Install git — required for ``pip install git+https://...`` of
# alpha-engine-lib below. The Lambda Python 3.12 base image does not
# include git; pip's git-cloning command fails with "Cannot find
# command 'git'" without this. Same fix applied to alpha-engine-research
# Dockerfile after PR #105's lib-public flip exposed the gap. Caught
# in alpha-engine-data 2026-05-05 when PR #159's deploy job failed
# with the same error. ``microdnf`` is the AL2023 minimal package
# manager; ``-y`` auto-confirms.
RUN microdnf install -y git && microdnf clean all

# Install dependencies. alpha-engine-lib is installed from public git+https
# with the [flow_doctor] extra only — the [arcticdb,rag] extras in
# requirements.txt are intentionally NOT pulled here, since Lambda only
# runs Phase 2 alternative-data collection (no ArcticDB or RAG ingestion).
# Excludes pytest / python-dotenv / pre-installed Lambda runtime deps
# (boto3 etc.). The grep filter strips alpha-engine-lib from the
# requirements file so the [flow_doctor]-only install above isn't
# overridden by the [arcticdb,flow_doctor,rag] extras pinned for EC2.
COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir "alpha-engine-lib[flow_doctor] @ git+https://github.com/cipher813/alpha-engine-lib@v0.15.0" && \
    grep -vE "^#|^$|^pytest|^python-dotenv|^boto3|^botocore|^s3transfer|^alpha-engine-lib" requirements.txt > /tmp/req-lambda.txt && \
    pip install --no-cache-dir -r /tmp/req-lambda.txt && \
    rm -rf /root/.cache/pip /tmp/req-lambda.txt

# Copy application code
COPY collectors/ ${LAMBDA_TASK_ROOT}/collectors/
COPY polygon_client.py ${LAMBDA_TASK_ROOT}/
COPY weekly_collector.py ${LAMBDA_TASK_ROOT}/
COPY store/ ${LAMBDA_TASK_ROOT}/store/

# flow-doctor.yaml at LAMBDA_TASK_ROOT is loaded by setup_logging() at
# module-top of lambda/handler.py. The path resolves via:
#   os.environ.get("LAMBDA_TASK_ROOT", os.path.dirname(...)) / "flow-doctor.yaml"
COPY flow-doctor.yaml ${LAMBDA_TASK_ROOT}/

# NOTE: config.yaml is intentionally NOT copied here. It is gitignored
# (contains bucket names + prefixes that we keep out of the public repo)
# so there's nothing to copy at build time, and lambda/handler.py already
# falls back to a hardcoded default ({"bucket": "alpha-engine-research",
# "market_data": {"s3_prefix": "market_data/"}}) when config.yaml is
# absent. Including it here was a dead COPY that broke `docker build`
# on every fresh checkout and blocked the auto-deploy workflow.

# Lambda handler
COPY lambda/handler.py ${LAMBDA_TASK_ROOT}/handler.py

CMD ["handler.handler"]
