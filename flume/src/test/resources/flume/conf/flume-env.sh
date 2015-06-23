#!/bin/bash

# This script runs on the machine which have maven repository populated under
# $HOME/.m2 If that's not the case, please adjust the JARPATH variable below
# to point to colon separated list of directories where jar files can be found
if test -z "$DT_FLUME_JAR"
then
  echo [ERROR]: Environment variable DT_FLUME_JAR should point to a valid jar file which contains DTFlumeSink class >&2
  exit 2
fi

echo JARPATH is set to ${JARPATH:=$HOME/.m2/repository:.}
if test -z "$JAVA_HOME"
then
  JAVA=java
else
  JAVA=${JAVA_HOME}/bin/java
fi
FLUME_CLASSPATH=`JARPATH=$JARPATH $JAVA -cp $DT_FLUME_JAR com.datatorrent.jarpath.JarPath -N $DT_FLUME_JAR -Xdt-jarpath -Xdt-netlet`