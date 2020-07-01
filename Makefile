VERSION=0.0.17
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
				--volume artifacts_$(NAME):/artifacts \
				--add-host bastion.localtest.nspcc.ru:192.168.123.10 \
				--add-host bastion.localtest.nspcc.ru:192.168.123.10 \
				--add-host cdn.fs.localtest.nspcc.ru:192.168.123.40 \
				--add-host main_chain.fs.localtest.nspcc.ru:192.168.123.50 \
				--add-host fs.localtest.nspcc.ru:192.168.123.20 \
				--add-host m01.fs.localtest.nspcc.ru:192.168.123.61 \
				--add-host m02.fs.localtest.nspcc.ru:192.168.123.62 \
				--add-host m03.fs.localtest.nspcc.ru:192.168.123.63 \
				--add-host m04.fs.localtest.nspcc.ru:192.168.123.64 \
				--add-host send.fs.localtest.nspcc.ru:192.168.123.30 \
				--add-host s01.fs.localtest.nspcc.ru:192.168.123.71 \
				--add-host s02.fs.localtest.nspcc.ru:192.168.123.72 \
				--add-host s03.fs.localtest.nspcc.ru:192.168.123.73 \
				--add-host s04.fs.localtest.nspcc.ru:192.168.123.74 \
				robot:$(VERSION)$(PREFIX) ./dockerd.sh &
	@sleep 10;
	@docker wait $(NAME);
	@echo "${B}${G}⇒ Testsuite has been completed. ${R}";
	@echo "${B}${G}⇒ Copy Logs from container to ./artifacts/ ${R}";
	@docker cp $(NAME):/artifacts .
	@docker rm $(NAME)

run:
	@echo "${B}${G}⇒ Test Run ${R}"
	@robot --timestampoutputs --outputdir artifacts/ robot/testsuites/integration/object_suite.robot 

help:
	@echo "${B}${G}⇒ build        Build image ${R}" 
	@echo "${B}${G}⇒ run          Run testcases ${R}" 
	@echo "${B}${G}⇒ run_docker   Run in docker ${R}" 

