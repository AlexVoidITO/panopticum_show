# Panopticum project

a project to find losses on power lines



## Want to use this project?

```sh
$ docker-compose up -d --build
$ docker-compose sh -c "echo 'Running migrations...' && alembic revision --autogenerate -m 'init tables in database' && alembic upgrade heads &&echo 'Migrations sucess'"

Sanity check: http://localhost:8004/health

