Flume
===============================

The folder contains support for flume to be used with Apex. It comprises mainly of two components. First is an agent that sits on the flume side, receives data from flume and makes it available via a socket server. In effect it converts a push to a pull model. The second component is the input operator that reads from the agent.

The project is started with the latest code at the time of the sub-module creation. For older history look at the flume sub-module in the older project called Megh (git@github.com:DataTorrent/Megh).


## Setup flume agent:

To set up the flume agent for Apex input operator, flumes plugin-based
architecture is used.

Set up flume and make sure JAVA_HOME is set.

Build malhar-flume `mvn clean package -DskipTests`.
The plugin `malhar-flume-ver.jar` and all necessary dependencies `target/deps` can now be found in the target directory.
To add the plugin to your flume service create a plugins.d directories in FLUME_HOME.

Put the malhar-flume-ver.jar in `plugins.d/custom-plugin-name/lib/`
and all the needed dependencies into `plugins.d/custom-plugin-name/libext/`

(Alternatively to flume's automatic plugins.d detection, jars can be added to the
FLUME_CLASSPATH using a `flume-env.sh` script. (See 'resources/flume-conf/flume-env.sample.sh')
Therefore a maven repository must be available under $HOME/.m2 and the environment variable
APEX_FLUME_JAR must point to the plugin JAR.)

***Flume configuration***  
A basic flume configuration can be found in `src/test/resources/flume/conf/flume_simple.conf`.  
A flume configuration using discovery service can be found in `src/test/resources/flume/conf/flume_zkdiscovery.conf`.  
  Configuration files should be placed in flumes 'conf' directory and will be explicitly selected
  when running flume-ng

In the configuration file set `org.apache.apex.malhar.flume.sink.FlumeSink` for the **type**  
and `org.apache.apex.malhar.flume.storage.HDFSStorage` for the **storage**,  
as well as a **HDFS directory** for `baseDir`. The HDFS base directory needs
to be created on HDFS.

For discovery set `org.apache.apex.malhar.flume.discovery.ZKAssistedDiscovery` for each sink
and configure them to use the zookeeper service by adding the zookeeper address in `connectionString` as well as a `basePath`.
These values also need to be set for **ZKListener** in the apex application.

### Operator Usage

An implementation of AbstractFlumeInputOperator can either simply connect
to one flume sink or use discovery/zookeeper to detect flume sinks automatically
and partition the operator accordingly at the beginning.

Implement abstract method to convert the Flume event to tuple:
```java
public abstract T convert(Event event);
```

Additionally a StreamCodec for Flume events must be set. A codec implementation
 can be found in storage/EventCodec.java
```java
setCodec(new EventCodec());
```

See `ApplicationDiscoveryTest.FlumeInputOperator` for an example operator implementation
##### Simple connection setup to one flume sink:
For a simple connection to only one flume sink set the connection address in the form of `sinkid:host:port`:
```java
public void setConnectAddresses(String[] specs)
```


##### Setup using discovery/zookeeper:
For a flume input operator to discover flume sinks and partition accordingly
a zookeeper service needs to be set up.

An implementation of AbstractFlumeInputOperator needs to initialize a ZKStatsListener.
It additionally needs to override **definePartitions** to setup ZKStatsListener, discover addresses using discover()
and set them in discoveredFlumeSinks before calling the parents definePartitions method.


See `src/test/java/org/apache/apex/malhar/flume/integration/ApplicationDiscoveryTest.java`
and `src/test/java/org/apache/apex/malhar/flume/integration/ApplicationTest.java`
for test implementations.