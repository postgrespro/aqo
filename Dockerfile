# Copyright (c) 2016-2019, Postgres Professional
FROM alpine
LABEL maintainer="Andrey V. Lepikhov <a.lepikhov@postgrespro.ru>"
ENV PG_VERSION 11.5
ENV PG_SHA256 7fdf23060bfc715144cbf2696cf05b0fa284ad3eb21f0c378591c6bca99ad180

# the home directory for the postgres user is not created by default
RUN set -ex; \
	postgresHome="$(getent passwd postgres)"; \
	postgresHome="$(echo "$postgresHome" | cut -d: -f6)"; \
	[ "$postgresHome" = '/var/lib/postgresql' ]; \
	mkdir -p "$postgresHome"; \
	chown -R postgres:postgres "$postgresHome"

# Install minimal set of packages
RUN apk add --update gcc libc-dev bison flex readline-dev zlib-dev perl make \
	diffutils gdb iproute2 musl-dbg iputils patch bash su-exec

RUN mkdir /pg && chown postgres:postgres pg

# Download corresponding version of PostgreSQL sources
RUN set -ex \
	\
	&& wget -O postgresql.tar.bz2 "https://ftp.postgresql.org/pub/source/v$PG_VERSION/postgresql-$PG_VERSION.tar.bz2" \
	&& echo "$PG_SHA256 *postgresql.tar.bz2" | sha256sum -c - \
	&& mkdir -p /pg/src \
	&& tar \
		--extract \
		--file postgresql.tar.bz2 \
		--directory /pg/src \
		--strip-components 1 \
	&& rm postgresql.tar.bz2

# Configure, compile and install
RUN mkdir /pg/src/contrib/aqo
COPY ./ /pg/src/contrib/aqo
RUN cd /pg/src/contrib/aqo && ls
RUN cd /pg/src && \
	patch -p1 --no-backup-if-mismatch < contrib/aqo/aqo_pg11.patch
RUN cd /pg/src && \
	./configure --enable-cassert --enable-debug --prefix=/pg/install && \
	make -j 4 install
RUN cd /pg/src/contrib/aqo && make clean && make install

# Set environment
ENV LANG en_US.utf8
ENV CFLAGS -O0
ENV PATH /pg/install/bin:$PATH
ENV PGDATA /pg/data

USER postgres
ENTRYPOINT ["/pg/src/contrib/aqo/docker-entrypoint.sh"]

EXPOSE 5432
CMD ["postgres"]
