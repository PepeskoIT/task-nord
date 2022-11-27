FROM python:3.10-slim

ARG APP_PATH
RUN echo $APP_PATH
RUN mkdir -p $APP_PATH
WORKDIR $APP_PATH

RUN apt-get clean \
    && apt-get update \
    # install stuff
    && apt-get install -y locales \
    python3-dev default-libmysqlclient-dev build-essential \
    # end of install
    && echo "en_US.UTF-8 UTF-8" > /etc/locale.gen \
    && locale-gen \
    && rm -rf /var/cache/apt

RUN pip3 install pipenv 
COPY Pipfile Pipfile.lock ./
RUN pipenv install --system --deploy --ignore-pipfile

COPY src/ .
COPY docker/entrypoint.sh .
RUN chmod +x entrypoint.sh

ENTRYPOINT ["/bin/sh", "./entrypoint.sh"]