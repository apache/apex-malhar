/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.contrib.cassandra;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceStability;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <p>This is used to specify the connection parameters that is used to process mutations for the
 * {@link AbstractUpsertOutputOperator}.
 * The Connection can only be set for one table in a given keyspace and as such is used to define the following
 * <ul>
 * <li>Connection Parameters</li>
 * <li>Defaults to be used in case the execution context tuple {@link UpsertExecutionContext} does not want to
 * set one explicitly.</li>
 * </ul></p>
 * <p>
 * Note that the {@link ConnectionBuilder} is used to build an instance of the ConnectionStateManager.
 * An instance of this class is typically instantiated by implementing the following inside
 * {@link AbstractUpsertOutputOperator} withConnectionBuilder() method.
 * </p>
 * <p> A typical implementation of the ConnectionBuilder would like this:
 * <pre>
 *
 *     public ConnectionStateManager.ConnectionBuilder withConnectionBuilder()
 *     {
 *       return ConnectionStateManager.withNewBuilder()
 *       .withSeedNodes("localhost") // format of host1:port;host2:port;host3:port
 *       .withClusterNameAs("Test Cluster") // mandatory param
 *       .withDCNameAs("datacenter1") // mandatory param
 *      .withTableNameAs("users") // mandatory param
 *       .withKeySpaceNameAs("unittests") // mandatory param
 *       .withdefaultConsistencyLevel(ConsistencyLevel.LOCAL_ONE); // Set if required. Default of LOCAL_QUORUM
 *       // Rest of the configs are initialized to sane defaults
 *     }
 * </pre>
 * </p>
 * <p>Please refer {@link ConnectionBuilder} for details about parameters that can be used to define the connection
 * and its default behaviour </p>
 *
 * @since 3.6.0
 */
@InterfaceStability.Evolving
public class ConnectionStateManager implements AutoCloseable, Serializable
{

  private static final long serialVersionUID = -6024334738016015213L;
  private static final long DEFAULT_MAX_DELAY_MS = 30000L;
  private static final long DEFAULT_BASE_DELAY_MS = 10000L;
  // Cassandra cluster name
  private final String clusterName;
  // Cassandra DC Name
  private final String dcName;
  // Seeds nodes. The format for specifying the host names are host1:port;host2:port
  private final String seedNodesStr;
  private final long baseDelayMs;
  private final long maxDelayMs;
  // Other operational constraints
  private final transient LoadBalancingPolicy loadBalancingPolicy;
  private final transient RetryPolicy retryPolicy;
  private final transient QueryOptions queryOptions;
  private final transient ReconnectionPolicy reconnectionPolicy;
  private final transient ProtocolVersion protocolVersion;
  private transient Logger LOG = LoggerFactory.getLogger(ConnectionStateManager.class);
  // Connection specific config elements
  private transient Cluster cluster;
  // Session specific metadata
  private transient Session session;
  private String keyspaceName;
  private String tableName;
  private int defaultTtlInSecs;
  private ConsistencyLevel defaultConsistencyLevel;
  // Standard defaults
  private boolean isTTLSet = false;

  private ConnectionStateManager(final ConnectionBuilder connectionBuilder)
  {
    checkNotNull(connectionBuilder, "Connection Builder passed in as Null");
    checkNotNull(connectionBuilder.clusterName, "Cluster Name not set for Cassandra");
    checkNotNull(connectionBuilder.dcName, "DataCenter Name not set for Cassandra");
    checkNotNull(connectionBuilder.seedNodesStr,
        "Seed nodes not set for Cassandra. Pattern is host1:port;host2:port");
    checkNotNull(connectionBuilder.keyspaceName, "Keyspace Name not set for Cassandra");
    checkNotNull(connectionBuilder.tableName, "Table Name not set for Cassandra");
    //Required params
    this.clusterName = connectionBuilder.clusterName;
    this.dcName = connectionBuilder.dcName;
    this.seedNodesStr = connectionBuilder.seedNodesStr;
    this.keyspaceName = connectionBuilder.keyspaceName;
    this.tableName = connectionBuilder.tableName;

    // optional params
    if (connectionBuilder.maxDelayMs != null) {
      maxDelayMs = connectionBuilder.maxDelayMs;
    } else {
      maxDelayMs = DEFAULT_MAX_DELAY_MS; // 30 seconds
    }
    if (connectionBuilder.baseDelayMs != null) {
      baseDelayMs = connectionBuilder.baseDelayMs;
    } else {
      baseDelayMs = DEFAULT_BASE_DELAY_MS; // 10 seconds
    }
    if (connectionBuilder.defaultTtlInSecs != null) {
      defaultTtlInSecs = connectionBuilder.defaultTtlInSecs;
      isTTLSet = true;
    }
    if (connectionBuilder.defaultConsistencyLevel != null) {
      defaultConsistencyLevel = connectionBuilder.defaultConsistencyLevel;
    } else {
      defaultConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM;
    }
    if (connectionBuilder.loadBalancingPolicy != null) {
      loadBalancingPolicy = connectionBuilder.loadBalancingPolicy;
    } else {
      loadBalancingPolicy = new TokenAwarePolicy(
        DCAwareRoundRobinPolicy.builder()
          .withLocalDc(dcName)
          .build());
    }
    if (connectionBuilder.retryPolicy != null) {
      this.retryPolicy = connectionBuilder.retryPolicy;
    } else {
      retryPolicy = DowngradingConsistencyRetryPolicy.INSTANCE;
    }
    if (connectionBuilder.queryOptions != null) {
      this.queryOptions = connectionBuilder.queryOptions;
    } else {
      this.queryOptions = new QueryOptions().setConsistencyLevel(defaultConsistencyLevel);
    }
    if (connectionBuilder.reconnectionPolicy != null) {
      this.reconnectionPolicy = connectionBuilder.reconnectionPolicy;
    } else {
      this.reconnectionPolicy = new ExponentialReconnectionPolicy(baseDelayMs, maxDelayMs);
    }
    if (connectionBuilder.protocolVersion != null) {
      this.protocolVersion = connectionBuilder.protocolVersion;
    } else {
      this.protocolVersion = ProtocolVersion.NEWEST_SUPPORTED;
    }
    establishSessionWithPolicies();
  }

  public static ConnectionBuilder withNewBuilder()
  {
    return new ConnectionBuilder();
  }

  private void establishSessionWithPolicies()
  {
    Cluster.Builder clusterBuilder = Cluster.builder();
    String[] seedNodesSplit = seedNodesStr.split(";");
    Collection<InetAddress> allSeeds = new ArrayList<>();
    Set<String> allKnownPorts = new HashSet<>();
    for (String seedNode : seedNodesSplit) {
      String[] nodeAndPort = seedNode.split(":");
      if (nodeAndPort.length > 1) {
        allKnownPorts.add(nodeAndPort[1]);
      }
      try {
        allSeeds.add(InetAddress.getByName(nodeAndPort[0]));
      } catch (UnknownHostException e) {
        LOG.error(" Error while trying to initialize the seed brokers for the cassandra cluster " + e.getMessage(), e);
      }
    }
    clusterBuilder = clusterBuilder.addContactPoints(allSeeds);
    clusterBuilder
      .withClusterName(clusterName)
      // We can fail if all of the nodes die in the local DC
      .withLoadBalancingPolicy(loadBalancingPolicy)
      // Want Strong consistency
      .withRetryPolicy(retryPolicy)
      // Tolerate some nodes down
      .withQueryOptions(queryOptions)
      // Keep establishing connections after detecting a node down
      .withReconnectionPolicy(reconnectionPolicy);
    // Finally initialize the cluster info
    if (allKnownPorts.size() > 0) {
      int shortlistedPort = Integer.parseInt(allKnownPorts.iterator().next());
      clusterBuilder = clusterBuilder.withPort(shortlistedPort);
    }
    cluster = clusterBuilder.build();
    Metadata metadata = cluster.getMetadata();
    LOG.info("Connected to cluster: \n" + metadata.getClusterName());
    for (Host host : metadata.getAllHosts()) {
      LOG.info(String.format("Datacenter: %s; Host: %s; Rack: %s\n",
          host.getDatacenter(), host.getAddress(), host.getRack()));
    }
    session = cluster.connect(keyspaceName);
  }

  @Override
  public void close()
  {
    if (session != null) {
      session.close();
    }
    if (cluster != null) {
      cluster.close();
    }
  }

  public Cluster getCluster()
  {
    return cluster;
  }

  public void setCluster(Cluster cluster)
  {
    this.cluster = cluster;
  }

  public Session getSession()
  {
    return session;
  }

  public void setSession(Session session)
  {
    this.session = session;
  }

  public String getKeyspaceName()
  {
    return keyspaceName;
  }

  public void setKeyspaceName(String keyspaceName)
  {
    this.keyspaceName = keyspaceName;
  }

  public String getTableName()
  {
    return tableName;
  }

  public void setTableName(String tableName)
  {
    this.tableName = tableName;
  }

  public int getDefaultTtlInSecs()
  {
    return defaultTtlInSecs;
  }

  public void setDefaultTtlInSecs(int defaultTtlInSecs)
  {
    this.defaultTtlInSecs = defaultTtlInSecs;
  }

  public boolean isTTLSet()
  {
    return isTTLSet;
  }

  public void setTTLSet(boolean TTLSet)
  {
    isTTLSet = TTLSet;
  }

  public static class ConnectionBuilder
  {

    private String clusterName;
    private String dcName;
    private String seedNodesStr;
    private Long baseDelayMs; // Class to enable check for nulls and is optional
    private Long maxDelayMs; // Class to enable check for nulls and is optional

    private String keyspaceName;
    private String tableName;
    private Integer defaultTtlInSecs; // Class to enable checks for nulls and is optional
    private ConsistencyLevel defaultConsistencyLevel;

    private LoadBalancingPolicy loadBalancingPolicy;
    private RetryPolicy retryPolicy;
    private QueryOptions queryOptions;
    private ReconnectionPolicy reconnectionPolicy;
    private ProtocolVersion protocolVersion;

    public static final String CLUSTER_NAME_IN_PROPS_FILE = "cluster.name";
    public static final String DC_NAME_IN_PROPS_FILE = "dc.name";
    public static final String KEYSPACE_NAME_IN_PROPS_FILE = "keyspace.name";
    public static final String TABLE_NAME_IN_PROPS_FILE = "table.name";
    public static final String SEEDNODES_IN_PROPS_FILE = "seednodes";

    public ConnectionBuilder withClusterNameAs(String clusterName)
    {
      this.clusterName = clusterName;
      return this;
    }

    public ConnectionBuilder withDCNameAs(String dcName)
    {
      this.dcName = dcName;
      return this;
    }

    /**
     * Used to specify the seed nodes of the target cassandra cluster. Format is
     * host1:port;host2:port;host3:port
     * @param seedNodesStr
     * @return The builder instance as initially created updated with this value
       */
    public ConnectionBuilder withSeedNodes(String seedNodesStr)
    {
      this.seedNodesStr = seedNodesStr;
      return this;
    }

    /**
     * Used to specify the base delay while trying to set a Connection attempt policy
     * @param baseDelayMillis
     * @return The builder instance as initially created updated with this value
       */
    public ConnectionBuilder withBaseDelayMillis(long baseDelayMillis)
    {
      this.baseDelayMs = baseDelayMillis;
      return this;
    }

    /**
     * Used to specify the maximum time that can elapse before which a connection is given up as a failure attmept
     * @param maxDelayMillis
     * @return The builder instance as initially created updated with this value
       */
    public ConnectionBuilder withMaxDelayMillis(long maxDelayMillis)
    {
      this.maxDelayMs = maxDelayMillis;
      return this;
    }

    public ConnectionBuilder withKeySpaceNameAs(String keyspaceName)
    {
      this.keyspaceName = keyspaceName;
      return this;
    }

    public ConnectionBuilder withTableNameAs(String tableName)
    {
      this.tableName = tableName;
      return this;
    }

    public ConnectionBuilder withdefaultTTL(Integer defaultTtlInSecs)
    {
      this.defaultTtlInSecs = defaultTtlInSecs;
      return this;
    }

    /**
     * Used to specify the default consistency level when executing the mutations on the cluster.
     * Default if not set is LOCAL_QUORUM. Can be overriden at the tuple level using {@link UpsertExecutionContext}
     * @param consistencyLevel
     * @return The builder instance as initially created updated with this value
       */
    public ConnectionBuilder withdefaultConsistencyLevel(ConsistencyLevel consistencyLevel)
    {
      this.defaultConsistencyLevel = consistencyLevel;
      return this;
    }

    /**
     * Used to define how the nodes in the cluster will be contacted for executing a mutation.
     * The following is the default behaviour if not set.
     * 1. Use a TokenAware approach i.e. the row key is used to decide the right node to execute the mutation
     *    on the target cassandra node. i.e. One of the R-1 replicas is used as the coordinator node.
     *    This effectively balances the traffic onto all nodes of the cassandra cluster for the given
     *    Operator instance. Of course this assumes the keys are evenly distributed in the cluster
     *    which is normally the case
     * 2. Overlay TokenAware with DC aware approach - The above token aware approach is further segmented to use only
     *    the local DC for the mutation executions. Cassandras multi-DC execution will take care of the cross DC
     *    replication thus achieving the lowest possible latencies for the given mutation of writes.
     *
     * Using this effectively removes the need for an extra implementation of the Partitioning logic of the Operator
     * Nor would we need any extra logic ( for most use cases ) for dynamic partitioning implementations as the
     * underlying driver normalizes the traffic pattern anyways.
     * @param loadBalancingPolicy
     * @return The builder instance as initially created updated with this value
       */
    public ConnectionBuilder withLoadBalancingPolicy(LoadBalancingPolicy loadBalancingPolicy)
    {
      this.loadBalancingPolicy = loadBalancingPolicy;
      return this;
    }

    /**
     * Used to specify how queries will need to be retried in case the current in progress one fails.
     * The default is to use a DowngradingConsistency Policy i.e. first LOCAL_QUORUM is attempted and if
     * there is a failure because less than RF/2-1 nodes are alive, it automatically switches to the Consistency Level
     * of LOCAL_ONE and so on. ( and hope that hint windows take care of the rest when the nodes come back up )
     * @param retryPolicy
     * @return The builder instance as initially created updated with this value
       */
    public ConnectionBuilder withRetryPolicy(RetryPolicy retryPolicy)
    {
      this.retryPolicy = retryPolicy;
      return this;
    }

    /**
     * Used to set various aspects for executing a given query / mutation.
     * The default is to use  LOCAL_QUORUM consistency for all mutation queries
     * @param queryOptions
     * @return The builder instance as initially created updated with this value
       */
    public ConnectionBuilder withQueryOptions(QueryOptions queryOptions)
    {
      this.queryOptions = queryOptions;
      return this;
    }

    /**
     * Used to decide how to establish a connection to the cluster in case the current one fails.
     * The default if not set is to use an ExponentialRetry Policy.
     * The baseDelay and maxDelay are the two time windows that are used to specify the retry attempts
     * in an exponential manner
     * @param reconnectionPolicy
     * @return The builder instance as initially created updated with this value
     */
    public ConnectionBuilder withReconnectionPolicy(ReconnectionPolicy reconnectionPolicy)
    {
      this.reconnectionPolicy = reconnectionPolicy;
      return this;
    }

    public ConnectionBuilder withProtocolVersion(ProtocolVersion protocolVersion)
    {
      this.protocolVersion = protocolVersion;
      return this;
    }

    protected ConnectionStateManager initialize()
    {
      ConnectionStateManager operatorConnection = new ConnectionStateManager(this);
      return operatorConnection;
    }

  }

}
