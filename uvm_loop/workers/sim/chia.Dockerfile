FROM ghcr.io/ucb-bar/chia:latest

USER root

ARG VERILATOR_VERSION=5.042

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates wget git autoconf flex bison help2man perl \
    python3 python3-dev g++ make libfl-dev z3 python3-yaml \
    && rm -rf /var/lib/apt/lists/*

# Python verification runtime for the generated Cocotb + pyUVM environment.
RUN python3 -m pip install --no-cache-dir \
    cocotb==2.1.0 \
    pyuvm \
    cocotb-coverage

# Install Verilator.
RUN git clone --depth 1 --branch v${VERILATOR_VERSION} \
        https://github.com/verilator/verilator.git /tmp/verilator \
    && cd /tmp/verilator \
    && autoconf \
    && ./configure \
    && make -j"$(nproc)" \
    && make install \
    && rm -rf /tmp/verilator

ENV PATH="/usr/local/bin:${PATH}"

# Install the simulation worker implementation into the ARGUS worker image.
# This file must be rebuilt into the image whenever workers/sim/run.py changes.
COPY workers/sim/run.py /workspace/workers/sim/run.py

# Fail the image build if the simulation worker was not copied correctly.
RUN test -f /workspace/workers/sim/run.py \
    && grep -q -- '--rtl' /workspace/workers/sim/run.py \
    && grep -q 'VERILOG_SOURCES' /workspace/workers/sim/run.py

USER ray
WORKDIR /workspace

CMD ["sleep", "infinity"]