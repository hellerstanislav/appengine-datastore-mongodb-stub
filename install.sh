#!/bin/bash

function showHelp() {
    echo
    echo "Usage:"
    echo "sh install.sh /PATH/TO/YOUR/APPENGINE/SDK/"
}

# check params
if [ $# -lt 1 ]; then
    echo "Not enough of params."
    showHelp
    exit 1
fi 

# check if valid
VALID=`ls -l $1 2>/dev/null | grep VERSION`
if [ -z "$VALID" ]; then
    echo "Provided path is not valid Google App Engine SDK path."
    echo "Please, enter the path ..SOMEPATH../google_appengine/"
    showHelp
    exit 1
fi

# check version
SDK_VER=`grep release $1/VERSION | cut -d\" -f2`
echo "SDK version $SDK_VER"
if [[ "$SDK_VER" != "1.7.7" ]]; then
    echo "Wrong version of SDK. Expected 1.7.7. Sorry."
    exit 1
fi

SDK=$1
SDK_GOOGLE=$1/google/
DATASTORE_PATH=$SDK_GOOGLE/appengine/datastore/
PATCHFILE=dev_appserver.patch
STUBFILE=datastore_mongodb_stub.py

echo "Copying datastore mongodb stub into SDK..."
cp $STUBFILE $DATASTORE_PATH
cp $PATCHFILE $SDK_GOOGLE
cd $SDK_GOOGLE
echo "Patching dev_appserver..."
patch -p1 < $PATCHFILE
rm $PATCHFILE
echo "Done."

