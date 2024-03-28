define VENV_template
venv.$(1): venv.$(1)/bin/activate venv.$(1)/bin/environment.sh

venv.$(1)/bin/activate:
	@echo "Creating $(1) venv in $$@ from $$<"
	python3.12 -m venv venv.$(1)
	. venv.$(1)/bin/activate && \
	pip3.12 install --upgrade pip && \
	pip3.12 install -U setuptools && \
	pip3.12 install -Ur requirements.txt
	@echo "Applying activate script patch"
	patch -b -d venv.$(1)/bin/ < build_assets/activate.patch

venv.$(1)/bin/environment.sh: | venv/$(1)/environment.sh
	ln -s ../../venv/$(1)/environment.sh venv.$(1)/bin/environment.sh

endef
