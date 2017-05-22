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
package org.apache.apex.malhar.flume.operator;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.flume.discovery.Discovery;
import org.apache.apex.malhar.flume.discovery.ZKAssistedDiscovery;
import org.apache.apex.malhar.flume.sink.Server;

import org.apache.flume.Event;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.Stats.OperatorStats;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.netlet.AbstractLengthPrependerClient;
import com.datatorrent.netlet.DefaultEventLoop;
import com.datatorrent.netlet.util.Slice;

import static java.lang.Thread.sleep;

/**
 * <p>
 * Abstract AbstractFlumeInputOperator class.</p>
 *
 * @param <T> Type of the output payload.
 * @since 0.9.2
 */
public abstract class AbstractFlumeInputOperator<T>
    implements InputOperator, Operator.ActivationListener<OperatorContext>, Operator.IdleTimeHandler,
    Operator.CheckpointListener, Partitioner<AbstractFlumeInputOperator<T>>
{
  public final transient DefaultOutputPort<T> output = new DefaultOutputPort<T>();
  public final transient DefaultOutputPort<Slice> drop = new DefaultOutputPort<Slice>();
  @NotNull
  private String[] connectionSpecs;
  @NotNull
  private StreamCodec<Event> codec;
  private final ArrayList<RecoveryAddress> recoveryAddresses;
  @SuppressWarnings("FieldMayBeFinal") // it's not final because that mucks with the serialization somehow
  private transient ArrayBlockingQueue<Slice> handoverBuffer;
  private transient int idleCounter;
  private transient int eventCounter;
  private transient DefaultEventLoop eventloop;
  private transient volatile boolean connected;
  private transient OperatorContext context;
  private transient Client client;
  private transient long windowId;
  private transient byte[] address;
  @Min(0)
  private long maxEventsPerSecond;
  //This is calculated from maxEventsPerSecond, App window count and streaming window size
  private transient long maxEventsPerWindow;

  public AbstractFlumeInputOperator()
  {
    handoverBuffer = new ArrayBlockingQueue<Slice>(1024 * 5);
    connectionSpecs = new String[0];
    recoveryAddresses = new ArrayList<RecoveryAddress>();
    maxEventsPerSecond = Long.MAX_VALUE;
  }

  @Override
  public void setup(OperatorContext context)
  {
    long windowDurationMillis = context.getValue(OperatorContext.APPLICATION_WINDOW_COUNT) *
        context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS);
    maxEventsPerWindow = (long)(windowDurationMillis / 1000.0 * maxEventsPerSecond);
    logger.debug("max-events per-second {} per-window {}", maxEventsPerSecond, maxEventsPerWindow);

    try {
      eventloop = new DefaultEventLoop("EventLoop-" + context.getId());
      eventloop.start();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public void activate(OperatorContext ctx)
  {
    if (connectionSpecs.length == 0) {
      logger.info("Discovered zero FlumeSink");
    } else if (connectionSpecs.length == 1) {
      for (String connectAddresse: connectionSpecs) {
        logger.debug("Connection spec is {}", connectAddresse);
        String[] parts = connectAddresse.split(":");
        eventloop.connect(new InetSocketAddress(parts[1], Integer.parseInt(parts[2])), client = new Client(parts[0]));
      }
    } else {
      throw new IllegalArgumentException(
          String.format("A physical %s operator cannot connect to more than 1 addresses!",
              this.getClass().getSimpleName()));
    }

    context = ctx;
  }

  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;
    idleCounter = 0;
    eventCounter = 0;
  }

  @Override
  public void emitTuples()
  {
    int i = handoverBuffer.size();
    if (i > 0 && eventCounter < maxEventsPerWindow) {

      while (--i > 0 && eventCounter < maxEventsPerWindow - 1) {
        final Slice slice = handoverBuffer.poll();
        slice.offset += 8;
        slice.length -= 8;
        T convert = convert((Event)codec.fromByteArray(slice));
        if (convert == null) {
          drop.emit(slice);
        } else {
          output.emit(convert);
        }
        eventCounter++;
      }

      final Slice slice = handoverBuffer.poll();
      slice.offset += 8;
      slice.length -= 8;
      T convert = convert((Event)codec.fromByteArray(slice));
      if (convert == null) {
        drop.emit(slice);
      } else {
        output.emit(convert);
      }
      eventCounter++;

      address = Arrays.copyOfRange(slice.buffer, slice.offset - 8, slice.offset);
    }
  }

  @Override
  public void endWindow()
  {
    if (connected) {
      byte[] array = new byte[Server.Request.FIXED_SIZE];

      array[0] = Server.Command.WINDOWED.getOrdinal();
      Server.writeInt(array, 1, eventCounter);
      Server.writeInt(array, 5, idleCounter);
      Server.writeLong(array, Server.Request.TIME_OFFSET, System.currentTimeMillis());

      logger.debug("wrote {} with eventCounter = {} and idleCounter = {}", Server.Command.WINDOWED, eventCounter, idleCounter);
      client.write(array);
    }

    if (address != null) {
      RecoveryAddress rAddress = new RecoveryAddress();
      rAddress.address = address;
      address = null;
      rAddress.windowId = windowId;
      recoveryAddresses.add(rAddress);
    }
  }

  @Override
  public void deactivate()
  {
    if (connected) {
      eventloop.disconnect(client);
    }
    context = null;
  }

  @Override
  public void teardown()
  {
    eventloop.stop();
    eventloop = null;
  }

  @Override
  public void handleIdleTime()
  {
    idleCounter++;
    try {
      sleep(context.getValue(OperatorContext.SPIN_MILLIS));
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }

  public abstract T convert(Event event);

  /**
   * @return the connectAddress
   */
  public String[] getConnectAddresses()
  {
    return connectionSpecs.clone();
  }

  /**
   * @param specs - sinkid:host:port specification of all the sinks.
   */
  public void setConnectAddresses(String[] specs)
  {
    this.connectionSpecs = specs.clone();
  }

  /**
   * @return the codec
   */
  public StreamCodec<Event> getCodec()
  {
    return codec;
  }

  /**
   * @param codec the codec to set
   */
  public void setCodec(StreamCodec<Event> codec)
  {
    this.codec = codec;
  }

  private static class RecoveryAddress implements Serializable
  {
    long windowId;
    byte[] address;

    @Override
    public String toString()
    {
      return "RecoveryAddress{" + "windowId=" + windowId + ", address=" + Arrays.toString(address) + '}';
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RecoveryAddress)) {
        return false;
      }

      RecoveryAddress that = (RecoveryAddress)o;

      if (windowId != that.windowId) {
        return false;
      }
      return Arrays.equals(address, that.address);
    }

    @Override
    public int hashCode()
    {
      int result = (int)(windowId ^ (windowId >>> 32));
      result = 31 * result + (address != null ? Arrays.hashCode(address) : 0);
      return result;
    }

    private static final long serialVersionUID = 201312021432L;
  }

  @Override
  public void checkpointed(long windowId)
  {
    /* dont do anything */
  }

  @Override
  public void committed(long windowId)
  {
    if (!connected) {
      return;
    }

    synchronized (recoveryAddresses) {
      byte[] addr = null;

      Iterator<RecoveryAddress> iterator = recoveryAddresses.iterator();
      while (iterator.hasNext()) {
        RecoveryAddress ra = iterator.next();
        if (ra.windowId > windowId) {
          break;
        }

        iterator.remove();
        if (ra.address != null) {
          addr = ra.address;
        }
      }

      if (addr != null) {
        /*
         * Make sure that we store the last valid address processed
         */
        if (recoveryAddresses.isEmpty()) {
          RecoveryAddress ra = new RecoveryAddress();
          ra.address = addr;
          recoveryAddresses.add(ra);
        }

        int arraySize = 1/* for the type of the message */
            + 8 /* for the location to commit */
            + 8 /* for storing the current time stamp*/;
        byte[] array = new byte[arraySize];

        array[0] = Server.Command.COMMITTED.getOrdinal();
        System.arraycopy(addr, 0, array, 1, 8);
        Server.writeLong(array, Server.Request.TIME_OFFSET, System.currentTimeMillis());
        logger.debug("wrote {} with recoveryOffset = {}", Server.Command.COMMITTED, Arrays.toString(addr));
        client.write(array);
      }
    }
  }

  @Override
  public Collection<Partition<AbstractFlumeInputOperator<T>>> definePartitions(
      Collection<Partition<AbstractFlumeInputOperator<T>>> partitions, PartitioningContext context)
  {
    Collection<Discovery.Service<byte[]>> discovered = discoveredFlumeSinks.get();
    if (discovered == null) {
      return partitions;
    }

    HashMap<String, ArrayList<RecoveryAddress>> allRecoveryAddresses = abandonedRecoveryAddresses.get();
    ArrayList<String> allConnectAddresses = new ArrayList<String>(partitions.size());
    for (Partition<AbstractFlumeInputOperator<T>> partition: partitions) {
      String[] lAddresses = partition.getPartitionedInstance().connectionSpecs;
      allConnectAddresses.addAll(Arrays.asList(lAddresses));
      for (int i = lAddresses.length; i-- > 0;) {
        String[] parts = lAddresses[i].split(":", 2);
        allRecoveryAddresses.put(parts[0], partition.getPartitionedInstance().recoveryAddresses);
      }
    }

    HashMap<String, String> connections = new HashMap<String, String>(discovered.size());
    for (Discovery.Service<byte[]> service: discovered) {
      String previousSpec = connections.get(service.getId());
      String newspec = service.getId() + ':' + service.getHost() + ':' + service.getPort();
      if (previousSpec == null) {
        connections.put(service.getId(), newspec);
      } else {
        boolean found = false;
        for (ConnectionStatus cs: partitionedInstanceStatus.get().values()) {
          if (previousSpec.equals(cs.spec) && !cs.connected) {
            connections.put(service.getId(), newspec);
            found = true;
            break;
          }
        }

        if (!found) {
          logger.warn("2 sinks found with the same id: {} and {}... Ignoring previous.", previousSpec, newspec);
          connections.put(service.getId(), newspec);
        }
      }
    }

    for (int i = allConnectAddresses.size(); i-- > 0;) {
      String[] parts = allConnectAddresses.get(i).split(":");
      String connection = connections.remove(parts[0]);
      if (connection == null) {
        allConnectAddresses.remove(i);
      } else {
        allConnectAddresses.set(i, connection);
      }
    }

    allConnectAddresses.addAll(connections.values());

    partitions.clear();
    try {
      if (allConnectAddresses.isEmpty()) {
        /* return at least one of them; otherwise stram becomes grumpy */
        @SuppressWarnings("unchecked")
        AbstractFlumeInputOperator<T> operator = getClass().newInstance();
        operator.setCodec(codec);
        operator.setMaxEventsPerSecond(maxEventsPerSecond);
        for (ArrayList<RecoveryAddress> lRecoveryAddresses: allRecoveryAddresses.values()) {
          operator.recoveryAddresses.addAll(lRecoveryAddresses);
        }
        operator.connectionSpecs = new String[allConnectAddresses.size()];
        for (int i = connectionSpecs.length; i-- > 0;) {
          connectionSpecs[i] = allConnectAddresses.get(i);
        }

        partitions.add(new DefaultPartition<AbstractFlumeInputOperator<T>>(operator));
      } else {
        long maxEventsPerSecondPerOperator = maxEventsPerSecond / allConnectAddresses.size();
        for (int i = allConnectAddresses.size(); i-- > 0;) {
          @SuppressWarnings("unchecked")
          AbstractFlumeInputOperator<T> operator = getClass().newInstance();
          operator.setCodec(codec);
          operator.setMaxEventsPerSecond(maxEventsPerSecondPerOperator);
          String connectAddress = allConnectAddresses.get(i);
          operator.connectionSpecs = new String[] {connectAddress};

          String[] parts = connectAddress.split(":", 2);
          ArrayList<RecoveryAddress> remove = allRecoveryAddresses.remove(parts[0]);
          if (remove != null) {
            operator.recoveryAddresses.addAll(remove);
          }

          partitions.add(new DefaultPartition<AbstractFlumeInputOperator<T>>(operator));
        }
      }
    } catch (IllegalAccessException ex) {
      throw new RuntimeException(ex);
    } catch (InstantiationException ex) {
      throw new RuntimeException(ex);
    }

    logger.debug("Requesting partitions: {}", partitions);
    return partitions;
  }

  @Override
  public void partitioned(Map<Integer, Partition<AbstractFlumeInputOperator<T>>> partitions)
  {
    logger.debug("Partitioned Map: {}", partitions);
    HashMap<Integer, ConnectionStatus> map = partitionedInstanceStatus.get();
    map.clear();
    for (Entry<Integer, Partition<AbstractFlumeInputOperator<T>>> entry: partitions.entrySet()) {
      if (map.containsKey(entry.getKey())) {
        // what can be done here?
      } else {
        map.put(entry.getKey(), null);
      }
    }
  }

  @Override
  public String toString()
  {
    return "AbstractFlumeInputOperator{" + "connected=" + connected + ", connectionSpecs=" +
        (connectionSpecs.length == 0 ? "empty" : connectionSpecs[0]) + ", recoveryAddresses=" + recoveryAddresses + '}';
  }

  class Client extends AbstractLengthPrependerClient
  {
    private final String id;

    Client(String id)
    {
      this.id = id;
    }

    @Override
    public void onMessage(byte[] buffer, int offset, int size)
    {
      try {
        handoverBuffer.put(new Slice(buffer, offset, size));
      } catch (InterruptedException ex) {
        handleException(ex, eventloop);
      }
    }

    @Override
    public void connected()
    {
      super.connected();

      byte[] address;
      synchronized (recoveryAddresses) {
        if (recoveryAddresses.size() > 0) {
          address = recoveryAddresses.get(recoveryAddresses.size() - 1).address;
        } else {
          address = new byte[8];
        }
      }

      int len = 1 /* for the message type SEEK */
          + 8 /* for the address */
          + 8 /* for storing the current time stamp*/;

      byte[] array = new byte[len];
      array[0] = Server.Command.SEEK.getOrdinal();
      System.arraycopy(address, 0, array, 1, 8);
      Server.writeLong(array, 9, System.currentTimeMillis());
      write(array);

      connected = true;
      ConnectionStatus connectionStatus = new ConnectionStatus();
      connectionStatus.connected = true;
      connectionStatus.spec = connectionSpecs[0];
      OperatorContext ctx = context;
      synchronized (ctx) {
        logger.debug("{} Submitting ConnectionStatus = {}", AbstractFlumeInputOperator.this, connectionStatus);
        context.setCounters(connectionStatus);
      }
    }

    @Override
    public void disconnected()
    {
      connected = false;
      ConnectionStatus connectionStatus = new ConnectionStatus();
      connectionStatus.connected = false;
      connectionStatus.spec = connectionSpecs[0];
      OperatorContext ctx = context;
      synchronized (ctx) {
        logger.debug("{} Submitting ConnectionStatus = {}", AbstractFlumeInputOperator.this, connectionStatus);
        context.setCounters(connectionStatus);
      }
      super.disconnected();
    }

  }

  public static class ZKStatsListner extends ZKAssistedDiscovery implements com.datatorrent.api.StatsListener,
      Serializable
  {
    /*
     * In the current design, one input operator is able to connect
     * to only one flume adapter. Sometime in future, we should support
     * any number of input operators connecting to any number of flume
     * sinks and vice a versa.
     *
     * Until that happens the following map should be sufficient to
     * keep track of which input operator is connected to which flume sink.
     */
    long intervalMillis;
    private final Response response;
    private transient long nextMillis;

    public ZKStatsListner()
    {
      intervalMillis = 60 * 1000L;
      response = new Response();
    }

    @Override
    public Response processStats(BatchedOperatorStats stats)
    {
      final HashMap<Integer, ConnectionStatus> map = partitionedInstanceStatus.get();
      response.repartitionRequired = false;

      Object lastStat = null;
      List<OperatorStats> lastWindowedStats = stats.getLastWindowedStats();
      for (OperatorStats os: lastWindowedStats) {
        if (os.counters != null) {
          lastStat = os.counters;
          logger.debug("Received custom stats = {}", lastStat);
        }
      }

      if (lastStat instanceof ConnectionStatus) {
        ConnectionStatus cs = (ConnectionStatus)lastStat;
        map.put(stats.getOperatorId(), cs);
        if (!cs.connected) {
          logger.debug("setting repatitioned = true because of lastStat = {}", lastStat);
          response.repartitionRequired = true;
        }
      }

      if (System.currentTimeMillis() >= nextMillis) {
        logger.debug("nextMillis = {}", nextMillis);
        try {
          super.setup(null);
          Collection<Discovery.Service<byte[]>> addresses;
          try {
            addresses = discover();
          } finally {
            super.teardown();
          }
          AbstractFlumeInputOperator.discoveredFlumeSinks.set(addresses);
          logger.debug("\ncurrent map = {}\ndiscovered sinks = {}", map, addresses);
          switch (addresses.size()) {
            case 0:
              response.repartitionRequired = map.size() != 1;
              break;

            default:
              if (addresses.size() == map.size()) {
                for (ConnectionStatus value: map.values()) {
                  if (value == null || !value.connected) {
                    response.repartitionRequired = true;
                    break;
                  }
                }
              } else {
                response.repartitionRequired = true;
              }
              break;
          }
        } catch (Error er) {
          throw er;
        } catch (Throwable cause) {
          logger.warn("Unable to discover services, using values from last successful discovery", cause);
        } finally {
          nextMillis = System.currentTimeMillis() + intervalMillis;
          logger.debug("Proposed NextMillis = {}", nextMillis);
        }
      }

      return response;
    }

    /**
     * @return the intervalMillis
     */
    public long getIntervalMillis()
    {
      return intervalMillis;
    }

    /**
     * @param intervalMillis the intervalMillis to set
     */
    public void setIntervalMillis(long intervalMillis)
    {
      this.intervalMillis = intervalMillis;
    }

    private static final long serialVersionUID = 201312241646L;
  }

  public static class ConnectionStatus implements Serializable
  {
    int id;
    String spec;
    boolean connected;

    @Override
    public int hashCode()
    {
      return spec.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final ConnectionStatus other = (ConnectionStatus)obj;
      return spec == null ? other.spec == null : spec.equals(other.spec);
    }

    @Override
    public String toString()
    {
      return "ConnectionStatus{" + "id=" + id + ", spec=" + spec + ", connected=" + connected + '}';
    }

    private static final long serialVersionUID = 201312261615L;
  }

  private static final transient ThreadLocal<HashMap<Integer, ConnectionStatus>> partitionedInstanceStatus =
      new ThreadLocal<HashMap<Integer, ConnectionStatus>>()
    {
      @Override
      protected HashMap<Integer, ConnectionStatus> initialValue()
      {
        return new HashMap<Integer, ConnectionStatus>();
      }

    };
  /**
   * When a sink goes away and a replacement sink is not found, we stash the recovery addresses associated
   * with the sink in a hope that the new sink may show up in near future.
   */
  private static final transient ThreadLocal<HashMap<String, ArrayList<RecoveryAddress>>> abandonedRecoveryAddresses =
      new ThreadLocal<HashMap<String, ArrayList<RecoveryAddress>>>()
  {
    @Override
    protected HashMap<String, ArrayList<RecoveryAddress>> initialValue()
    {
      return new HashMap<String, ArrayList<RecoveryAddress>>();
    }

  };
  protected static final transient ThreadLocal<Collection<Discovery.Service<byte[]>>> discoveredFlumeSinks =
      new ThreadLocal<Collection<Discovery.Service<byte[]>>>();

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AbstractFlumeInputOperator)) {
      return false;
    }

    AbstractFlumeInputOperator<?> that = (AbstractFlumeInputOperator<?>)o;

    if (!Arrays.equals(connectionSpecs, that.connectionSpecs)) {
      return false;
    }
    return recoveryAddresses.equals(that.recoveryAddresses);

  }

  @Override
  public int hashCode()
  {
    int result = connectionSpecs != null ? Arrays.hashCode(connectionSpecs) : 0;
    result = 31 * result + (recoveryAddresses.hashCode());
    return result;
  }

  public void setMaxEventsPerSecond(long maxEventsPerSecond)
  {
    this.maxEventsPerSecond = maxEventsPerSecond;
  }

  public long getMaxEventsPerSecond()
  {
    return maxEventsPerSecond;
  }

  private static final Logger logger = LoggerFactory.getLogger(AbstractFlumeInputOperator.class);
}
