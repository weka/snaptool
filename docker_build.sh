./build_pybin.sh
VERSION=$(./tarball/snaptool/snaptool --version | awk '{print $3}')
docker build --tag wekasolutions/snaptool:$VERSION --tag wekasolutions/snaptool:latest .
