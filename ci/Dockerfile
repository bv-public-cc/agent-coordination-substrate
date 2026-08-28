# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e
FROM nexus.internal-domain.scrubbed:5000/ci/python@sha256:54d8146bd58adb458cafab0aee96800fd5bd45f30f36884f667e783ea4108750

RUN apt-get update \
    && apt-get install --yes --no-install-recommends make=4.3-4.1 \
    && rm -rf /var/lib/apt/lists/*

# Nexus credentials are supplied as a BuildKit secret. They are never copied
# into the layer or retained in image history.
RUN --mount=type=secret,id=pip_index,required=true \
    PIP_INDEX_URL="$(cat /run/secrets/pip_index)" \
    python -m pip install --no-cache-dir --disable-pip-version-check PyYAML==6.0.2 \
    && python -c "import yaml; assert yaml.__version__ == '6.0.2'"
