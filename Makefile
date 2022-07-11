#!/usr/bin/make -f

.DEFAULT_GOAL := help

SHELL = bash

OUTPUT_DIR = artifacts/
KEYWORDS_REPO = git@github.com:nspcc-dev/neofs-keywords.git
VENVS = $(shell ls -1d venv/*/ | sort -u | xargs basename -a)
ROOT_DIR := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
DEV_IMAGE_PY ?= registry.spb.yadro.com/tools/pytest-neofs-x86_64:6

ifdef DEVENV_SERVICES_PATH
	RUN_OPTIONS += -v $(DEVENV_SERVICES_PATH)/chain/node-wallet.json:/wallets/mainnet.json -e MAINNET_WALLET_PATH_HOST="/wallets/mainnet.json" 
	RUN_OPTIONS += -v $(DEVENV_SERVICES_PATH)/ir/wallet01.json:/wallets/ir.json -e IR_WALLET_PATH="/wallets/ir.json" 
endif

ifeq ($(shell uname -s),Darwin)
	DOCKER_NETWORK = --network bridge -p 389:389 -p 636:636
endif


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

pytest-local:
	@echo "⇒ Run Pytest"
	export PYTHONPATH=$(ROOT_DIR)/neofs-keywords/lib:$(ROOT_DIR)/neofs-keywords/robot:$(ROOT_DIR)/robot/resources/lib:$(ROOT_DIR)/robot/resources/lib/python_keywords:$(ROOT_DIR)/robot/variables && \
	python -m pytest pytest_tests/testsuites/

help:
	@echo "⇒ run          Run testcases ${R}"
