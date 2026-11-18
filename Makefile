.PHONY: check deep bench fmt-check lint test wasm proof verdict lean

BEND := $(shell command -v bend || echo $(HOME)/.bend/bin/bend)
LAKE := $(shell command -v lake || echo $(HOME)/.elan/bin/lake)

check: fmt-check lint test wasm proof

fmt-check:
	cargo fmt --all --check

lint:
	cargo clippy --workspace --all-targets --all-features --locked -- -D warnings

test:
	cargo test --workspace --all-features --locked

wasm:
	cargo build -p fongo-sync-protocol --target wasm32-unknown-unknown --locked

proof:
	@$(BEND) PROOF.bend | tee /dev/stderr | grep -q '^ALL PROOFS CHECK'

# The long runs: the property test at thousands of cases and the exhaustive test at larger
# bounds, optimised. `make deep SEED=n` repeats a run; every case is a function of the seed.
SEED ?= $(shell date +%s)
deep:
	@echo "make deep SEED=$(SEED)"
	PROPTEST_RNG_SEED=$(SEED) PROPTEST_CASES=4000 \
		cargo test --release --locked -p fongo-sync-sqlite --test property
	EXHAUSTIVE_STEPS=4 cargo test --release --locked -p fongo-sync-sqlite --test exhaustive -- --nocapture
	EXHAUSTIVE_REPLICAS=3 EXHAUSTIVE_STEPS=3 EXHAUSTIVE_SEEDS=2 \
		cargo test --release --locked -p fongo-sync-sqlite --test exhaustive -- --nocapture

# Timings over a large library, printed as a table.
bench:
	cargo test --release --locked -p fongo-sync-sqlite --test bench -- --ignored --nocapture

# Not part of `check`: it needs a Lean toolchain (see lean/lean-toolchain).
lean:
	cd lean && $(LAKE) build
	@! grep -rnwE 'sorry|admit|axiom|native_decide' lean/FongoSync lean/FongoSync.lean
	@cd lean && $(LAKE) env lean Audit.lean | tee /dev/stderr | { ! grep -q sorryAx; }

# Rechecks the proofs with BendTT's kernel, which needs Lean on PATH.
verdict:
	@$(BEND) PROOF.bend --verdict | tee /dev/stderr | grep -q '^ALL PROOFS CHECK'
