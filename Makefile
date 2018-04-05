VENV := precommit_venv
HOOKS := .git/hooks/pre-commit

# PRE-COMMIT HOOKS

$(VENV): .requirements-precommit.txt
	rm -rf $(VENV)
	virtualenv -p python3.6 $(VENV)
	$(VENV)/bin/pip install -i https://pypi.yelpcorp.com/simple/ -r .requirements-precommit.txt

.PHONY: env
env: $(VENV)

.PHONY: clean-env
clean-env:
	rm -rf $(VENV)

.PHONY: install-hooks
install-hooks: $(VENV) .pre-commit-config.yaml
	$(VENV)/bin/pre-commit install -f --install-hooks
	cargo fmt --help > /dev/null || rustup component add rustfmt-preview

.PHONY: clean-hooks
clean-hooks:
	rm -rf $(HOOKS)


# LINTING

.PHONY: lint
lint:
	 cargo +nightly fmt --all

.PHONY: clean-lint
clean-lint:
	find . -type f -name *.rs.bk -delete

# ALL
.PHONY: clean
clean: clean-env clean-hooks clean-lint
	cargo clean
