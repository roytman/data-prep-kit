# Define the root of the local git clone for the common rules to be able
# know where they are running from.
REPOROOT=../

# Include the common rules.
# Use "make help" to see them.
include ${REPOROOT}/.make.defaults

# Command to run pytest
PYTHON_VERSION=$(shell $(PYTHON) --version)
VENV_ACTIVATE=venv/bin/activate


clean::
	@# Help: Clean up the distribution build and the venv
	rm -r dist venv || true
	rm -rf src/*egg-info || true
	rm -rf *.back || true

.PHONY: venv
venv:
	rm -rf venv
	@# Help: Create the virtual environment using pyproject.toml
	$(MAKE) .defaults.python-lib-src-venv
	. ${VENV_ACTIVATE}; pip install -e .