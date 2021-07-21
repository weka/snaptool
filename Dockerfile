FROM alpine:latest

RUN apk add --no-cache bash curl python3 py3-pip tzdata

RUN pip3 install pyyaml python-dateutil urllib3 wekalib

ARG BASEDIR="/weka"
ARG ID="472"
ARG USER="weka"

RUN mkdir -p $BASEDIR

WORKDIR $BASEDIR

COPY snaptool.py $BASEDIR
COPY snaptool.yml $BASEDIR
COPY snapshots.py $BASEDIR
COPY background.py $BASEDIR

RUN addgroup -S -g $ID $USER &&\
    adduser -S -h $BASEDIR -u $ID -G $USER $USER && \
    chown -R $USER:$USER $BASEDIR

RUN chmod +x $BASEDIR/snaptool

USER $USER
ENTRYPOINT ["./snaptool"]
