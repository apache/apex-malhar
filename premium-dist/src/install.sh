#!/bin/bash

# pull the dependencies from the embedded repo copy into the local repo
BASE_DIR=$( cd $(dirname $0); pwd -P )
REPO_DIR=$BASE_DIR/maven2
mvn -DgroupId=com.malhartech -DartifactId=malhar-daemon -Dversion=0.1-SNAPSHOT -Dtransitive=true -DrepoUrl=file://$REPO_DIR \
 org.apache.maven.plugins:maven-dependency-plugin:2.5.1:get

mvn dependency:build-classpath \
 -Dmdep.outputFile="$BASE_DIR/bin/mvn-generated-runtime-classpath" \
 -Dmdep.includeScope="runtime" -Dmdep.excludeScope="test" \
 -f $BASE_DIR/project-template/pom.xml

# cleanup the (invalid) local repository reference for update check not to fail
# this hack can be removed once we publish our artifacts in a public repository
# more info: http://maven.40175.n5.nabble.com/Maven-3-maven-repositories-and-lastUpdated-td4927537.html
find ~/.m2 -name "*_maven*" | grep malhar | xargs rm

mkdir -p $HOME/.stram

if [ ! -f $HOME/.stram/stram-site.xml ] 
then
    ipaddrs=`ifconfig -a | grep 'inet addr' | sed -e 's/.*inet addr:\([0-9\.]*\) .*/\1/'`
    for ipaddr in $ipaddrs
    do
	first=`echo $ipaddr | cut -f 1 -d .`
	second=`echo $ipaddr | cut -f 2 -d .`
	if [ \( $first -eq 10 \) -o \
	    \( \( $first -eq 172 \) -a \( $second -ge 16 \) -a \( $second -le 31 \) \) -o \
	    \( \( $first -eq 192 \) -a \( $second -eq 168 \) \) ]
	then
	    selected_ipaddr=$ipaddr
	fi
    done

    if [ "$selected_ipaddr" ]
    then
	echo "Selected IP is $selected_ipaddr"
    else
	echo "Cannot determine NAT IP address. Please specify the daemon address in $HOME/.stam/stram-site.xml"
	selected_ipaddr="myhostipaddr"
    fi

    sed "s/my\.host\.ip\.addr/$selected_ipaddr/" $BASE_DIR/sample-stram-site.xml | sed "s!/my/htdocs/dir!$BASE_DIR/htdocs!" > $HOME/.stram/stram-site.xml
fi
