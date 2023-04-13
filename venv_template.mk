define VENV_template
venv.$(1): venv.$(1)/bin/activate venv.$(1)/bin/environment.sh

venv.$(1)/bin/activate:
	@echo "Creating $(1) venv in $$@ from $$<"
	virtualenv --python=python3.10 --prompt="($(1))" venv.$(1)
	. venv.$(1)/bin/activate && \
	pip3.10 install -U setuptools==56.0.0 && \
	pip3.10 install -Ur requirements.txt
	@echo "Applying activate script patch"
	patch -R --dry-run -p1 -s -f -d venv.$(1)/bin/ < build_assets/activate.patch || \
	patch -p1 -d venv.$(1)/bin/ < build_assets/activate.patch

venv.$(1)/bin/environment.sh: | venv/$(1)/environment.sh
	ln -s ../../venv/$(1)/environment.sh venv.$(1)/bin/environment.sh

endef
