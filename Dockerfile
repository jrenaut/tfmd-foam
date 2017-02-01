FROM postgres
RUN mkdir /code
WORKDIR /code
ADD requirements.txt /code/
RUN apt-get update
RUN apt-get install -y python-pip libpq-dev python-psycopg2 git-core
RUN pip install -r requirements.txt
ADD . /code/
