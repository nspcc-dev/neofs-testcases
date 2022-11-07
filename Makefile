#!/usr/bin/make -f

.DEFAULT_GOAL := help

SHELL ?= bash

VENVS = $(shell ls -1d venv/*/ | sort -u | xargs basename -a)

.PHONY: all
all: venvs

include venv_template.mk

.PHONY: venvs
venvs:
	$(foreach venv,$(VENVS),venv.$(venv))

$(foreach venv,$(VENVS),$(eval $(call VENV_template,$(venv))))

clean:
	rm -rf venv.*

pytest-local:
	@echo "⇒ Run Pytest"
	python -m pytest pytest_tests/testsuites/

help:
	@echo "⇒ run          Run testcases ${R}"
