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
# docker run --mount type=bind,source="$(pwd)"/output,target=/app my_app
# docker run --rm -v output:/app/output manifold_data_process
# docker run --rm --mount source=my_test_volume,target=/app manifold_data_process
	docker run --rm -v $(PWD)/Test_volume:/app/my_test_volume manifold_data_process



test:
	docker run --rm manifold_data_process_test
# pytest

volume:
	docker volume create --name my_test_volume --opt type=none --opt device=./Test_volume --opt o=bind


testtest:
	pylint process --good-names=i,j,df ||:


