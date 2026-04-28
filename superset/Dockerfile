FROM apache/superset:3.1.3

USER root

# apt packages (gettext-base for envsubst, zip to repack dashboard export).
# Retry up to 5 times to survive transient 5xx from Debian mirrors.
RUN for i in 1 2 3 4 5; do \
      apt-get update && \
      apt-get install -y --no-install-recommends gettext-base zip && \
      break || { echo "apt attempt $i failed, retrying..."; sleep 5; }; \
    done \
    && rm -rf /var/lib/apt/lists/*

# Trino SQLAlchemy driver
RUN pip install --no-cache-dir \
      sqlalchemy-trino==0.5.0 \
      trino==0.328.0

# Superset config (SECRET_KEY from env, etc.)
COPY superset_config.py /app/pythonpath/superset_config.py
ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py

# Entrypoint
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh \
    && chown superset:superset /app/entrypoint.sh

USER superset

EXPOSE 8088
ENTRYPOINT ["/app/entrypoint.sh"]
