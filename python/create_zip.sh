#!/bin/bash
VERSION=$1
echo $1
cd apex-python/src
zip -r pyapex-$VERSION-src.zip pyapex
mv pyapex-$VERSION-src.zip ../deps
cd ../../
