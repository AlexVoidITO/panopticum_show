# pull official base image
FROM python:3.13-slim-bookworm

# set working directory
WORKDIR /usr/src/app

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1


# install system dependencies
# install python dependencies
RUN pip install --upgrade pip setuptools wheel
RUN pip install --upgrade pip
COPY ./requirements.txt .
RUN pip install -r requirements.txt

# add app
COPY . .

CMD ["echo",  "'app started"]
#alembic revision --autogenerate -m "init" && alembic upgrade head  