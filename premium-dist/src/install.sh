#!/bin/bash

# pull the dependencies from the embedded repo copy into the local repo
BASE_DIR=$( cd $(dirname $0); pwd -P )
REPO_DIR=$BASE_DIR/maven2
mvn -DgroupId=com.malhartech -DartifactId=malhar-daemon -Dversion=0.1-SNAPSHOT -Dtransitive=true -DrepoUrl=file://$REPO_DIR \
 org.apache.maven.plugins:maven-dependency-plugin:2.5.1:get

# cleanup the (invalid) local repository reference for update check not to fail
# this hack can be removed once we publish our artifacts in a public repository
# more info: http://maven.40175.n5.nabble.com/Maven-3-maven-repositories-and-lastUpdated-td4927537.html
find ~/.m2 -name "*_maven*" | grep malhar | xargs rm

mvn dependency:build-classpath \
 -Dmdep.outputFile="$BASE_DIR/bin/mvn-generated-runtime-classpath" \
 -Dmdep.includeScope="runtime" -Dmdep.excludeScope="test" \
 -f $BASE_DIR/project-template/pom.xml


