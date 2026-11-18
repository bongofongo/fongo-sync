.PHONY: check fmt-check lint test wasm lean lean-diff

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
