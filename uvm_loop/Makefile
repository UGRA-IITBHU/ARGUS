BENCHMARK ?= fifo
DESIGN_CONFIG ?= benchmarks/$(BENCHMARK)/design.yaml
BENCHMARK_ROOT ?= generated/designs/$(BENCHMARK)
TB_DIR := $(BENCHMARK_ROOT)/tb
EXAMPLE_RTL ?= $(shell python3 -c "import yaml; c=yaml.safe_load(open('$(DESIGN_CONFIG)')); print(c['rtl'])")
TOP ?= adder_tb_top
BUILD_DIR ?= $(BENCHMARK_ROOT)/build-smoke

.PHONY: pipeline sim clean build-sim-image sim-generated

pipeline:
	python3 -m pipeline.run14 --design-config $(DESIGN_CONFIG)

# Quick structural smoke-test with plain Verilator.
sim:
	mkdir -p $(BUILD_DIR)
	verilator --binary -j 0 \
		-I$(TB_DIR) \
		--top-module $(TOP) \
		$(EXAMPLE_RTL) \
		$(TB_DIR)/*.sv \
		-Mdir $(BUILD_DIR) \
		--Wno-fatal
	./$(BUILD_DIR)/V$(TOP)

clean:
	rm -rf generated/designs
	mkdir -p generated/designs

# Build the Ray-based simulation worker image.
build-sim-image:
	docker build -f workers/sim/chia.Dockerfile -t chia-sim-worker:chia-local .

# Run the standalone simulation worker for one benchmark.
sim-generated:
	mkdir -p $(BENCHMARK_ROOT)/results
	docker run --rm \
		-v "$(PWD):/workspace" \
		chia-sim-worker:chia-local \
		--rtl /workspace/$(shell python3 -c "import yaml; c=yaml.safe_load(open('$(DESIGN_CONFIG)')); print(c['rtl'])") \
		--tb /workspace/$(TB_DIR) \
		--output /workspace/$(BENCHMARK_ROOT)/results/manual_simulation_result.json \
		--test-timeout 60 \
		--build-timeout 1800
