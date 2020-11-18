VERSION=0.0.18
PREFIX=

B=\033[0;1m
G=\033[0;92m
R=\033[0m

.DEFAULT_GOAL := help
.PHONY: build-image  

DATE = $(shell date +%s)
NAME = "testcases_$(DATE)"

build:
	@echo "${B}${G}⇒ Build image ${R}"
	@docker build \
		 --build-arg REG_USR=$(REG_USR) \
		 --build-arg REG_PWD=$(REG_PWD) \
		 --build-arg JF_TOKEN=$(JF_TOKEN) \
		 --build-arg BUILD_NEOFS_NODE=${BUILD_NEOFS_NODE} \
		 --build-arg BUILD_CLI=${BUILD_CLI} \
		 -f Dockerfile \
		 -t robot:$(VERSION)$(PREFIX) .
 
run_docker:
	@echo "${B}${G}⇒ Test Run image $(NAME)${R}"
	@mkdir artifacts_$(NAME)
	@docker run --privileged=true  \
				--name $(NAME) \
				 robot:$(VERSION)$(PREFIX) ./dockerd.sh &
	@sleep 10;
	@docker wait $(NAME);
	@echo "${B}${G}⇒ Testsuite has been completed. ${R}";
	@echo "${B}${G}⇒ Copy Logs from container to ./artifacts/ ${R}";
	@docker cp $(NAME):/artifacts .
	@docker rm $(NAME)

run:
	@echo "${B}${G}⇒ Test Run ${R}"
	@robot --timestampoutputs --outputdir artifacts/ robot/testsuites/integration/*.robot 

help:
	@echo "${B}${G}⇒ build        Build image ${R}" 
	@echo "${B}${G}⇒ run          Run testcases ${R}" 
	@echo "${B}${G}⇒ run_docker   Run in docker ${R}" 

