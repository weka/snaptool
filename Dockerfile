FROM ubuntu:20.04

ARG SNAPTOOL_BIN="./tarball/snaptool/snaptool"
ARG BIN_DST="/wekabin"

ARG ID="472"
ARG USER="weka"
ARG HOMEDIR="/weka"

RUN mkdir -p $BIN_DST

COPY $SNAPTOOL_BIN $BIN_DST

RUN adduser --home $HOMEDIR --uid $ID --disabled-password --gecos "Weka User" $USER
RUN chown $USER $BIN_DST

WORKDIR $HOMEDIR
USER $USER
CMD ["--version"]
ENTRYPOINT ["/wekabin/snaptool"]