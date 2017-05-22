#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#


# This script runs on the machine which have maven repository populated under
# $HOME/.m2 If that's not the case, please adjust the JARPATH variable below
# to point to colon separated list of directories where jar files can be found
if test -z "$APEX_FLUME_JAR"
then
  echo [ERROR]: Environment variable APEX_FLUME_JAR should point to a valid jar file which contains FlumeSink class >&2
  exit 2
fi

echo JARPATH is set to ${JARPATH:=$HOME/.m2/repository:.}
if test -z "$JAVA_HOME"
then
  JAVA=java
else
  JAVA=${JAVA_HOME}/bin/java
fi
FLUME_CLASSPATH=`JARPATH=$JARPATH $JAVA -cp $APEX_FLUME_JAR com.datatorrent.jarpath.JarPath -N $APEX_FLUME_JAR -Xdt-jarpath -Xdt-netlet`