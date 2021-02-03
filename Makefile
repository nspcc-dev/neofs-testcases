.DEFAULT_GOAL := help

run:
	@echo "⇒ Test Run"
	@robot --timestampoutputs --outputdir artifacts/ robot/testsuites/integration/ 

help:
	@echo "⇒ run          Run testcases ${R}" 

