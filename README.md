# Panopticum project

a project to find losses on power lines

## Note: Currently, this version of the repository does not contain the main business logic for security reasons. only the structure of the services themselves is presented here.

## Want to use this project?

```sh
$ docker-compose up -d --build
$ docker-compose sh -c "echo 'Running migrations...' && alembic revision --autogenerate -m 'init tables in database' && alembic upgrade heads &&echo 'Migrations sucess'"

Sanity check: http://localhost:8004/health

