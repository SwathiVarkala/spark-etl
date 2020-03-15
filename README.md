## Command to build and deploy containers
`docker-compose build && docker-compose up --force-recreate`

Note: Make sure docker is running in your machine. if not installed, follow https://docs.docker.com/docker-for-mac/install for mac
### This will deploy following containers
* spark master with two worker nodes (spark is in stand alone mode). spark master will also contain livy server in it
* postgres db for airflow metadata
* airflow webserver
* aiflow scheduler

### How to access these services?
* spark master web ui - localhost:8080
* spark history server - localhost:18080
* livy server - localhost:8998
* airflow web ui - localhost:8090
