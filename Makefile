.PHONY: assemble docker-build docker-deps migrate local

assemble:
	./gradlew clean assemble

docker-build:
	docker-compose build

docker-deps:
	docker-compose up localstack postgres mockserver -d
	sleep 3 # wait for deps to be up

migrate:
	./gradlew migrateLocal

local: assemble docker-build docker-deps migrate
	docker-compose up app || docker-compose down -v # clean everything on exit
