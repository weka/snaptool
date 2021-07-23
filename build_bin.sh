# this file uses pyinstaller to create the binary tarball that is used to deploy snaptool
# this allows snaptool to be deployed without installing python and other required python packages
#
TOOL=snaptool
MAIN=snaptool.py
pyinstaller --onefile $MAIN

TARGET=tarball/$TOOL
mkdir -p $TARGET
cp dist/$TOOL $TARGET
cp snaptool.yml $TARGET
cp snaptool.service $TARGET
cd tarball
tar cvzf ../${TOOL}.tar $TOOL
