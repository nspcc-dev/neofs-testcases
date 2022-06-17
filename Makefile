#!/usr/bin/make -f

.DEFAULT_GOAL := help

SHELL = bash

OUTPUT_DIR = artifacts/
KEYWORDS_REPO = git@github.com:nspcc-dev/neofs-keywords.git
VENVS = $(shell ls -1d venv/*/ | sort -u | xargs basename -a)
ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
DEV_IMAGE_PY             ?= registry.spb.yadro.com/tools/pytest-neofs-x86_64:3

ifeq ($(shell uname -s),Darwin)
	DOCKER_NETWORK         = --network bridge -p 389:389 -p 636:636
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

.PHONY: pytest-docker
pytest-docker:
	-docker ps
	-docker rm   neofs_tests_py
	-docker pull $(DEV_IMAGE_PY)
	docker run -t --rm                                  \
		--name neofs_tests_py							\
		-e PYTHONPATH="/root/neofs-keywords/lib:/root/neofs-keywords/robot:/root/robot/resources/lib:/root/robot/resources/lib/python_keywords:/root/robot/variables:/root/pytest_tests/helpers"		\
		-v $(CURDIR):/root			 					\
		-v /var/run/docker.sock:/var/run/docker.sock	\
		-v $(NEO_BIN_DIR):/neofs						\
		--privileged									\
		$(DOCKER_NETWORK)								\
		--env-file $(CURDIR)/.env						\
		$(DEV_IMAGE_PY)									\
		-v 												\
		-m "$(CI_MARKERS)"								\
		--color=no										\
		--junitxml=/root/xunit_results.xml				\
		--alluredir=/root/allure_results 				\
		--setup-show									\
		/root/pytest_tests/testsuites
