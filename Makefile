.DEFAULT_GOAL := help

run:
	@echo "⇒ Test Run"
	@robot --timestampoutputs --outputdir artifacts/ robot/testsuites/integration/*.robot 

help:
	@echo "⇒ run          Run testcases ${R}" 

