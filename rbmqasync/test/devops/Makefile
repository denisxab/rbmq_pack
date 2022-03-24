
env_path=./__env.env
include $(env_path)
export POSTGRES_HOST, PATH_ENV, POSTGRES_CONTAINER_NAME, RABBITMQ_CONTAINER_NAME


docker_build:
	sudo docker-compose --env-file $(PATH_ENV) build

docker_up:
	sudo docker-compose --env-file $(PATH_ENV)  up && sudo docker-compose rm -fsv

docker_info:
	 sudo docker-compose --env-file $(PATH_ENV) ps

docker_id_postgres:
	sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $(POSTGRES_CONTAINER_NAME)

docker_id_rabbitmq:
	sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $(RABBITMQ_CONTAINER_NAME)

docker_restart_postgres:
	sudo docker restart $(POSTGRES_CONTAINER_NAME)
