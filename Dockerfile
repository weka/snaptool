FROM alpine:latest

RUN apk add --no-cache bash curl python3 py3-pip tzdata

RUN pip3 install pyyaml python-dateutil urllib3 wekalib

ARG BINSRC="./tarball/snaptool/snaptool"
ARG BINDST="/usr/loca/bin/snaptool"
ARG BASEDIR="/weka"
ARG ID="472"
ARG USER="weka"

RUN mkdir -p $BASEDIR

COPY $BINSRC $BINDST

WORKDIR $BASEDIR

RUN addgroup -S -g $ID $USER &&\
    adduser -S -h $BASEDIR -u $ID -G $USER $USER && \
    chown -R $USER:$USER $BASEDIR &&

USER $USER
CMD ["-c", "snaptool.yml"]
ENTRYPOINT ["/usr/local/bin/snaptool"]
