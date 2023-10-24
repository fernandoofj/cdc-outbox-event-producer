.PHONY: assemble docker-build docker-deps local

assemble:
	./gradlew clean assemble

docker-build:
	docker-compose build

docker-deps:
	docker-compose up localstack postgres -d
	sleep 3 # wait for deps to be up

local: assemble docker-build docker-deps
	docker-compose down -v # clean everything on exit
