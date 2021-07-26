FROM alpine:latest

RUN apk add --no-cache bash curl python3 py3-pip tzdata

RUN pip3 install pyyaml python-dateutil urllib3 wekalib

ARG BIN="./tarball/snaptool/snaptool"
ARG BASEDIR="/weka"
ARG LOGDIR="/weka/logs"
ARG ID="472"
ARG USER="weka"

RUN mkdir -p $BASEDIR
RUN mkdir -p $LOGDIR

WORKDIR $BASEDIR

COPY $BIN $BASEDIR
COPY snaptool-example.yml $BASEDIR/snaptool.yml

RUN addgroup -S -g $ID $USER &&\
    adduser -S -h $BASEDIR -u $ID -G $USER $USER && \
    chown -R $USER:$USER $BASEDIR

USER $USER
CMD ["-c", "snaptool.yml"]
ENTRYPOINT ["snaptool"]
