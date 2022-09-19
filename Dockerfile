# pull official base image
FROM python:3.9-slim-buster as base

ENV PTYTHONPATH=/app:$PYTHONPATH

# set working directory
RUN mkdir -p /app
WORKDIR /app
RUN cd /app

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# add and install requirements
COPY ./requirements.txt .
RUN python3 -m pip install -r requirements.txt

# add app
COPY . .

CMD [ "python", "./process/process2.py"]

FROM base as test
COPY ./test-requirements.txt .
RUN python3 -m pip install -r test-requirements.txt

# Pylint always returns an exit code, `||:` effectivley supresses it.
CMD pylint process --good-names=i,j,df ||:
