FROM bmeares/meerschaum

RUN mrsm install plugin compose

COPY --chown=meerschaum:meerschaum ./ /app
WORKDIR /app

### Install required packages if you've added project plugins.
RUN MRSM_PLUGINS_DIR='/app/plugins' mrsm show plugins && \
    PLUGINS=$(mrsm show plugins --nopretty) && \
    [ "$PLUGINS" ] && \
    mrsm compose install required || true

ENTRYPOINT ["/app/docker/bootstrap.sh"]
