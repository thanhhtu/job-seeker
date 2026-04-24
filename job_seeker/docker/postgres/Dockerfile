FROM postgres:17

RUN apt-get update && apt-get install -y \
    build-essential \
    postgresql-server-dev-17 \
    git \
    && rm -rf /var/lib/apt/lists/* \
    && git clone https://github.com/timescale/pg_textsearch /tmp/pg_textsearch \
    && cd /tmp/pg_textsearch \
    && make OPTFLAGS="" \
    && make install \
    && cd / \
    && rm -rf /tmp/pg_textsearch \
    && git clone https://github.com/pgvector/pgvector /tmp/pgvector \
    && cd /tmp/pgvector \
    && make OPTFLAGS="" \
    && make install \
    && cd / \
    && rm -rf /tmp/pgvector \
    && apt-get purge -y build-essential postgresql-server-dev-17 git \
    && apt-get autoremove -y \
    && apt-get clean
    
RUN echo "shared_preload_libraries = 'pg_textsearch'" >> /usr/share/postgresql/postgresql.conf.sample
