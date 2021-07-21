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

