This is an alpha release not for production use.

The archive contains all artifacts needed to develop a streaming application on the Malhar platform.

Documentation is under: 

<UNZIP_DIR>/docs/index.html

Requirements for development/runtime environment:

- Operating system Linux (should also work on OS X)
- Oracle Java 6 or Java 7 (or OpenJDK)
- Maven 3 (http://maven.apache.org)
- Hadoop 2.x cluster to run applications in distributed mode
- Chrome to access the dashboard UI

Install the package by running <UNZIP_DIR>/install.sh

This will populate the local maven repository with required dependencies
and generate the classpath needed by the application launcher.


########################################
# To run demos

Start the command line interface (CLI):

<UNZIP_DIR>/bin/stramcli

To run demos in local mode:

launch-local <UNZIP_DIR>/maven2/com/malhartech/malhar-demos/0.1-SNAPSHOT/malhar-demos-0.1-SNAPSHOT.jar

Note that in local mode everything runs in the CLI process. You may need to
adjust the heap space depending on the application. Heap size can be set
with environment variable, default is STRAM_CLIENT_OPTS=-Xmx1024m

To run demos on your Hadoop 2.x cluster (you must have HADOOP_PREFIX set):

launch <UNZIP_DIR>/maven2/com/malhartech/malhar-demos/0.1-SNAPSHOT/malhar-demos-0.1-SNAPSHOT.jar


########################################
# To launch the frontend server (daemon)

Before starting the daemon for the first time, verify the listening address.
It is configured in ~/stram/stram-site.xml as stram.daemon.address

To start the daemon:

bin/start-malhar-daemon

To stop the daemon:

bin/stop-malhar-daemon

Access the front end page here:
http://[stram.daemon.address]/static/index.html


########################################
# To develop your own application

The archive contains an offline maven repository with all dependencies needed for the Malhar platform:

<UNZIP_DIR>/maven2

A project skeleton to get your own project started is under:

<UNZIP_DIR>/project-template

To initialize your local maven repository:

cd project-template
mvn package

This will pull the dependencies from the included repository to your local repository.

Import/open the project with the IDE of your choice (it should have maven support).
Eclipse users can generate project using: mvn eclipse:eclipse

Run the application skeleton through the unit test in local mode within the IDE.


Have fun writing your own Hadoop streaming application!

