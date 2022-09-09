.PHONY: init help test reqs process

objects = $(wildcard *.in)
outputs := $(objects:.in=.txt)

help:
	@echo "Manifold Data Processing Application!"
	@echo ""
	@echo "Author: Jeff Witmer"
	@echo "'make build' to build the Docker image."
	@echo "'make process' to start the data processing."
	@echo "'make test'" to run unit tests."
# @echo "'make config' to change defaults"
# @echo "'make run' to run local webserver version"
# @echo "'make build' to, um, build"

apt:
	sudo apt install awscli python3-pytest
	echo "Remove me to rerun apt" > apt

init:
# docker-compose build
# docker-compose up -d

build:
	docker build -t manifold_data_process --target base .
	docker build -t manifold_data_process_test --target test .

process:
	docker run --rm -it -v $(PWD)/output:/app/output manifold_data_process

test:
	docker run --rm -it manifold_data_process_test
# pytest



testtest:
	pylint process --good-names=i,j,df ||:


