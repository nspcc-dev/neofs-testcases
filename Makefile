.DEFAULT_GOAL := help

OUTPUT_DIR = artifacts/
KEYWORDS_PATH = ../neofs-keywords
KEYWORDS_REPO = git@github.com:nspcc-dev/neofs-keywords.git

run: deps
	@echo "⇒ Test Run"
	@robot --timestampoutputs --outputdir $(OUTPUT_DIR) robot/testsuites/integration/

deps: $(KEYWORDS_PATH)

$(KEYWORDS_PATH):
	@echo "Cloning keywords repo"
	@git clone $(KEYWORDS_REPO) $(KEYWORDS_PATH)

help:
	@echo "⇒ run          Run testcases ${R}"
