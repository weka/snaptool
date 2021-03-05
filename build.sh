TOOL=snaptool
pyinstaller --onefile $TOOL

TARGET=tarball/$TOOL
mkdir -p $TARGET
cp dist/$TOOL $TARGET
cp snaptool.yml $TARGET
cd tarball
tar cvzf ../${TOOL}.tar $TOOL

