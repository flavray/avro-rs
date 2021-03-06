VENV := precommit_venv
HOOKS := .git/hooks/pre-commit

# PRE-COMMIT HOOKS

$(VENV): .requirements-precommit.txt
	virtualenv -p python3 $(VENV)
	$(VENV)/bin/pip install -r .requirements-precommit.txt

.PHONY: env
env: $(VENV)

.PHONY: clean-env
clean-env:
	rm -rf $(VENV)

$(HOOKS): $(VENV) .pre-commit-config.yaml
	$(VENV)/bin/pre-commit install -f --install-hooks
	cargo fmt --help > /dev/null || rustup component add rustfmt
	cargo clippy --help > /dev/null || rustup component add clippy
	cargo readme --help > /dev/null || cargo install cargo-readme

.PHONY: install-hooks
install-hooks: $(HOOKS)

.PHONY: clean-hooks
clean-hooks:
	rm -rf $(HOOKS)

# LINTING

.PHONY: lint
lint:
	 cargo fmt

.PHONY: clean-lint
clean-lint:
	find . -type f -name *.rs.bk -delete

.PHONY: clippy
clippy: install-hooks
	cargo clippy --all-features --all-targets -- -Dclippy::all -Dunused_imports

# TESTING

.PHONY: test
test: install-hooks
	cargo test --all-features --all-targets
	# because of https://github.com/rust-lang/cargo/issues/6669
	cargo test --doc
	$(VENV)/bin/pre-commit run --all-files

# BENCHMARKING

.PHONY: benchmark
benchmark:
	cargo bench

# DOCS

.PHONY: doc
doc:
	cargo doc --no-deps --all-features

.PHONY: doc-local
doc-local:
	cargo doc --no-deps --all-features --open

.PHONY: readme
readme:
	cargo readme > README.md


# BUILDING

.PHONY: build
build:
	cargo build --all-features

.PHONY: release
release:
	cargo build --all-features --release

# CLEAN
#
.PHONY: clean
clean: clean-env clean-hooks clean-lint
	cargo clean
