.PHONY: check deep bench fmt-check lint test wasm lean lean-diff

LAKE := $(shell command -v lake || echo $(HOME)/.elan/bin/lake)

check: fmt-check lint test wasm lean lean-diff

fmt-check:
	cargo fmt --all --check

lint:
	cargo clippy --workspace --all-targets --all-features --locked -- -D warnings

test:
	cargo test --workspace --all-features --locked

wasm:
	cargo build -p fongo-sync-protocol --target wasm32-unknown-unknown --locked

# The long runs: the property test at thousands of cases and the exhaustive test at larger
# bounds, optimised, each run whatever the others found. `make deep SEED=n` repeats a run;
# every case is a function of the seed.
SEED ?= $(shell date +%s)
deep:
	@echo "make deep SEED=$(SEED)"; status=0; \
	PROPTEST_RNG_SEED=$(SEED) PROPTEST_CASES=4000 \
		cargo test --release --locked -p fongo-sync-sqlite --test property || status=1; \
	EXHAUSTIVE_STEPS=4 \
		cargo test --release --locked -p fongo-sync-sqlite --test exhaustive -- --nocapture || status=1; \
	EXHAUSTIVE_REPLICAS=3 EXHAUSTIVE_STEPS=3 EXHAUSTIVE_SEEDS=2 \
		cargo test --release --locked -p fongo-sync-sqlite --test exhaustive -- --nocapture || status=1; \
	exit $$status

# Timings over a large library, printed as a table.
bench:
	cargo test --release --locked -p fongo-sync-sqlite --test bench -- --ignored --nocapture

# Builds the proofs in lean/ (toolchain in lean/lean-toolchain) and audits Laws.lean.
lean:
	cd lean && $(LAKE) build
	@! grep -rnwE 'sorry|admit|axiom|native_decide' lean/FongoSync lean/FongoSync.lean
	@grep -oE '^theorem [^ ]+' lean/FongoSync/Laws.lean \
		| sed 's/^theorem /#print axioms FongoSync.Laws./' > lean/.lake/laws.txt
	@grep '^#print axioms' lean/Audit.lean | cmp -s - lean/.lake/laws.txt \
		|| { echo 'lean/Audit.lean must list exactly the theorems of lean/FongoSync/Laws.lean' >&2; exit 1; }
	@cd lean && $(LAKE) env lean Audit.lean > .lake/axioms.txt && cat .lake/axioms.txt
	@! grep -oE '\[.*\]' lean/.lake/axioms.txt | tr -d '[] ' | tr ',' '\n' \
		| grep -vxE 'propext|Classical\.choice|Quot\.sound'

# Runs fixed cases through the Lean view and through model::view, and compares the output.
lean-diff:
	cd lean && $(LAKE) build cases
	lean/.lake/build/bin/cases > lean/.lake/cases-lean.txt
	cargo run --quiet -p fongo-sync-core --example view_cases --locked > lean/.lake/cases-rust.txt
	diff -u lean/.lake/cases-lean.txt lean/.lake/cases-rust.txt
