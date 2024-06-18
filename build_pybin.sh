# this file uses pyinstaller to create the binary tarball that is used to deploy snaptool
# this allows snaptool to be deployed without installing python and other required python packages
#
TOOL=snaptool
MAIN=snaptool.py
TARGET=tarball/$TOOL

source venv/bin/activate

pyinstaller --onefile --add-data "templates:templates" --add-data "static:static" $MAIN

mkdir -p $TARGET
cp dist/$TOOL $TARGET
cp snaptool-example.yml $TARGET
cp snaptool-example.yml $TARGET/snaptool.yml
cp snaptool.service $TARGET
cp docker_run.sh $TARGET
chmod +xx $TARGET/docker_run.sh
cp install.sh $TARGET
chmod +xx $TARGET/install.sh

cd tarball
tar cvzf ../${TOOL}.tar $TOOL

