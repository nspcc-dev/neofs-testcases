#!/usr/bin/make -f

VENV_DIR := venv.pytest
PYTHON := python3.12
PIP := pip3.12
ENV_FILE := .env
BINARIES := neo-go neofs-adm neofs-cli neofs-ir neofs-lens neofs-node neofs-rest-gw neofs-s3-authmate neofs-s3-gw

SHELL ?= bash

.PHONY: all
all: venv.pytest

.PHONY: venv.pytest
venv.pytest:
	@echo "Creating virtual environment in $(VENV_DIR)..."
	$(PYTHON) -m venv $(VENV_DIR)
	@echo "Modifying activate script to add custom environment variables..."
	@while read -r line; do \
		echo "export $$line" >> $(VENV_DIR)/bin/activate; \
	done < $(ENV_FILE)
	@echo "Installing dependencies"
	. $(VENV_DIR)/bin/activate && \
	$(PIP) install --upgrade pip && \
	$(PIP) install -U setuptools && \
	$(PIP) install -Ur requirements.txt
	@echo "Virtual environment created and customized."

clean:
	@echo "Removing binaries: $(BINARIES)"
	rm -f $(BINARIES)
	@echo "Removing python venv"
	rm -rf venv.*
