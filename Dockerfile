FROM ubuntu:20.04
RUN apt-get -y update
RUN apt-get -y install nginx
RUN git clone https://git.postgresql.org/git/postgresql.git
RUN git clone https://github.com/postgrespro/aqo.git postgresql/contrib/aqo

EXPOSE 5432
CMD ["postgres"]