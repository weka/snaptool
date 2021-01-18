FROM alpine:latest

RUN apk add --no-cache bash curl python3 py3-pip

RUN pip3 install pyyaml python-dateutil

ARG BASEDIR="/weka"
ARG ID="472"
ARG USER="weka"

RUN mkdir -p $BASEDIR

WORKDIR $BASEDIR

COPY weka_snapd $BASEDIR
COPY weka_snapd.yml $BASEDIR
COPY snapshots.py $BASEDIR
COPY collector.py $BASEDIR
COPY circular.py $BASEDIR
COPY signals.py $BASEDIR
COPY sthreads.py $BASEDIR
COPY wekaapi.py $BASEDIR
COPY wekacluster.py $BASEDIR
COPY wekatime.py $BASEDIR

RUN addgroup -S -g $ID $USER &&\
    adduser -S -h $BASEDIR -u $ID -G $USER $USER && \
    chown -R $USER:$USER $BASEDIR

RUN chmod +x $BASEDIR/weka_snapd

USER $USER
ENTRYPOINT ["./weka_snapd"]
