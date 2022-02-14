#!/usr/bin/make -f

.DEFAULT_GOAL := help

SHELL = bash

OUTPUT_DIR = artifacts/
KEYWORDS_REPO = git@github.com:nspcc-dev/neofs-keywords.git
VENVS = $(shell ls -1d venv/*/ | sort -u | xargs basename -a)

.PHONY: all
all: venvs

include venv_template.mk

run: venvs
	@echo "⇒ Test Run"
	@robot --timestampoutputs --outputdir $(OUTPUT_DIR) robot/testsuites/integration/

.PHONY: venvs
venvs:
	$(foreach venv,$(VENVS),venv.$(venv))

$(foreach venv,$(VENVS),$(eval $(call VENV_template,$(venv))))

submodules:
	@git submodule init
	@git submodule update --recursive --remote

clean:
	rm -rf venv.*

help:
	@echo "⇒ run          Run testcases ${R}"
