/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.operator;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import static java.lang.Thread.sleep;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.*;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Partitionable.PartitionAware;
import com.datatorrent.api.Stats.OperatorStats;
import com.datatorrent.api.Stats.OperatorStats.CustomStats;

import com.datatorrent.flume.discovery.Discovery.Service;
import com.datatorrent.flume.discovery.ZKAssistedDiscovery;
import com.datatorrent.flume.sink.Server;
import com.datatorrent.flume.sink.Server.Command;
import com.datatorrent.netlet.AbstractLengthPrependerClient;
import com.datatorrent.netlet.DefaultEventLoop;

/**
 *
 * @param <T> Type of the output payload.
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public abstract class AbstractFlumeInputOperator<T>
        implements InputOperator, ActivationListener<OperatorContext>, IdleTimeHandler, CheckpointListener,
        Partitionable<AbstractFlumeInputOperator<T>>, PartitionAware<AbstractFlumeInputOperator<T>>
{
  public final transient DefaultOutputPort<T> output = new DefaultOutputPort<T>();
  private transient int idleCounter;
  private transient int eventCounter;
  private transient DefaultEventLoop eventloop;
  private transient RecoveryAddress recoveryAddress;
  private final transient ArrayBlockingQueue<Payload> handoverBuffer;
  private transient volatile boolean connected;
  private transient OperatorContext context;
  private transient Client client;

  @NotNull
  private String[] connectionSpecs;
  private final ArrayList<RecoveryAddress> recoveryAddresses;

  public AbstractFlumeInputOperator()
  {
    this.handoverBuffer = new ArrayBlockingQueue<Payload>(1024 * 5);
    this.recoveryAddresses = new ArrayList<RecoveryAddress>();
  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      eventloop = new DefaultEventLoop("EventLoop-" + context.getId());
      eventloop.start();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public void activate(OperatorContext ctx)
  {
    if (connectionSpecs.length != 1) {
      throw new RuntimeException(String.format("A physical {} operator cannot connect to more than 1 addresses!", this.getClass().getSimpleName()));
    }
    for (String connectAddresse: connectionSpecs) {
      String[] parts = connectAddresse.split(":");
      eventloop.connect(new InetSocketAddress(parts[1], Integer.parseInt(parts[2])), client = new Client(parts[0]));
    }

    context = ctx;
  }

  @Override
  public void beginWindow(long windowId)
  {
    recoveryAddress = new RecoveryAddress();
    recoveryAddress.windowId = windowId;
    idleCounter = 0;
    eventCounter = 0;
  }

  @Override
  public void emitTuples()
  {
    for (int i = handoverBuffer.size(); i-- > 0;) {
      Payload payload = handoverBuffer.poll();
      output.emit(payload.payload);
      recoveryAddress.address = payload.location;
      eventCounter++;
    }
  }

  @Override
  public void endWindow()
  {
    if (connected) {
      byte[] array = new byte[9];

      array[0] = Command.WINDOWED.getOrdinal();
      Server.writeInt(array, 1, eventCounter);
      Server.writeInt(array, 5, idleCounter);

      logger.debug("wrote {} with eventCounter = {} and idleCounter = {}", Command.WINDOWED, eventCounter, idleCounter);
      client.write(array);
    }

    recoveryAddresses.add(recoveryAddress);
  }

  @Override
  public void deactivate()
  {
    eventloop.disconnect(client);
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
      sleep(5);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }

  public abstract T convert(byte[] buffer, int offset, int size);

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

  private static class RecoveryAddress implements Serializable
  {
    long windowId;
    long address;
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

    Iterator<RecoveryAddress> iterator = recoveryAddresses.iterator();
    while (iterator.hasNext()) {
      RecoveryAddress ra = iterator.next();
      if (ra.windowId < windowId) {
        iterator.remove();
      }
      else if (ra.windowId == windowId) {
        iterator.remove();
        int arraySize = 1/* for the type of the message */
                        + 8 /* for the location to commit */;
        byte[] array = new byte[arraySize];

        array[0] = Command.COMMITTED.getOrdinal();

        final long recoveryOffset = ra.address;
        Server.writeLong(array, 1, recoveryOffset);

        logger.debug("wrote {} with recoveryOffset = {}", Command.COMMITTED, recoveryOffset);
        client.write(array);
      }
      else {
        break;
      }
    }
  }

  @Override
  public Collection<Partition<AbstractFlumeInputOperator<T>>> definePartitions(Collection<Partition<AbstractFlumeInputOperator<T>>> partitions, int incrementalCapacity)
  {
//        HashMap<String, Integer> accounted = new HashMap<String, Integer>(1);
//        for (SocketAddress address: addresses) {
//          int port = ((InetSocketAddress)address).getPort();
//          String host = ((InetSocketAddress)address).getHostName();
//
//          boolean found = false;
//          for (Entry<Integer, ConnectionStatus> entry: map.entrySet()) {
//            if (entry.getValue().port == port && host.equals(entry.getValue().host)) {
//              accounted.put(host + ':' + port, entry.getKey());
//              found = true;
//              break;
//            }
//          }
//
//          if (!found) {
//            accounted.put(host + ':' + port, null);
//          }
//    /* This is possible when the partitioning is advised by statslistener */
    Collection<Service<byte[]>> discovered = addresses.get();
    if (discovered == null && incrementalCapacity == 0) {
      return partitions;
    }

    ArrayList<String> allConnectAddresses = new ArrayList<String>(partitions.size() + incrementalCapacity);
    ArrayList<ArrayList<RecoveryAddress>> allRecoveryAddresses = new ArrayList<ArrayList<RecoveryAddress>>(partitions.size() + incrementalCapacity);
    for (Partition<AbstractFlumeInputOperator<T>> partition: partitions) {
      String[] lAddresses = partition.getPartitionedInstance().connectionSpecs;
      allConnectAddresses.addAll(Arrays.asList(lAddresses));
      for (int i = lAddresses.length; i-- > 0;) {
        allRecoveryAddresses.add(partition.getPartitionedInstance().recoveryAddresses);
      }
    }

    if (discovered != null) {
      HashMap<String, String> connections = new HashMap<String, String>(discovered.size());
      for (Service<byte[]> service: discovered) {
        String previousSpec = connections.get(service.getId());
        String newspec = service.getId() + ':' + service.getHost() + ':' + service.getPort();
        if (previousSpec == null) {
          connections.put(service.getId(), newspec);
        }
        else {
          boolean found = false;
          for (ConnectionStatus cs: localMap.get().values()) {
            if (previousSpec.equals(cs.spec) && !cs.connected) {
              connections.put(service.getId(), newspec);
              found = true;
              break;
            }
          }

          if (!found) {
            logger.warn("2 sinks found with the same id: {} and {}... Ignoring previous", previousSpec, newspec);
            connections.put(service.getId(), newspec);
          }
        }
      }

      for (int i = allConnectAddresses.size(); i-- > 0;) {
        String[] parts = allConnectAddresses.get(i).split(":");
        String get = connections.get(parts[0]);
        if (get == null) {
          allConnectAddresses.remove(i);
          allRecoveryAddresses.remove(i);
        }
        else {
          allConnectAddresses.set(i, get);
        }
      }
    }

    partitions.clear();

    try {
      for (int i = allConnectAddresses.size(); i-- > 0;) {
        @SuppressWarnings("unchecked")
        AbstractFlumeInputOperator<T> operator = getClass().newInstance();
        operator.connectionSpecs = new String[] {allConnectAddresses.get(i)};
        operator.recoveryAddresses.addAll(allRecoveryAddresses.get(i));
        partitions.add(new DefaultPartition<AbstractFlumeInputOperator<T>>(operator));
      }
    }
    catch (Error er) {
      throw er;
    }
    catch (RuntimeException re) {
      throw re;
    }
    catch (IllegalAccessException ex) {
      throw new RuntimeException(ex);
    }
    catch (InstantiationException ex) {
      throw new RuntimeException(ex);
    }

    return partitions;
  }

  @Override
  public void partitioned(Map<Integer, Partition<AbstractFlumeInputOperator<T>>> partitions)
  {
    HashMap<Integer, ConnectionStatus> map = localMap.get();
    for (Entry<Integer, Partition<AbstractFlumeInputOperator<T>>> entry: partitions.entrySet()) {
      if (map.containsKey(entry.getKey())) {
        // what can be done here?
      }
      else {
        map.put(entry.getKey(), null);
      }
    }
  }

  private class Payload
  {
    final T payload;
    final long location;

    Payload(T payload, long location)
    {
      this.payload = payload;
      this.location = location;
    }

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
      /* this are all the payload messages */
      Payload payload = new Payload(convert(buffer, offset + 8, size - 8), Server.readLong(buffer, 0));
      try {
        handoverBuffer.put(payload);
      }
      catch (InterruptedException ex) {
        handleException(ex, eventloop);
      }
    }

    @Override
    @SuppressWarnings("SynchronizeOnNonFinalField") /* context is virtually final for a given operator */

    public void connected()
    {
      super.connected();

      long address;
      if (recoveryAddresses.size() > 0) {
        address = recoveryAddresses.get(recoveryAddresses.size() - 1).address;
      }
      else {
        address = 0;
      }

      int len = 1 /* for the message type SEEK */
                + 8 /* for the address */;

      byte[] array = new byte[len];
      array[0] = Command.SEEK.getOrdinal();
      Server.writeLong(array, 1, address);
      write(array);

      connected = true;
      ConnectionStatus connectionStatus = new ConnectionStatus();
      connectionStatus.connected = true;
      connectionStatus.spec = connectionSpecs[0];
      synchronized (context) {
        context.setCustomStats(connectionStatus);
      }
      logger.debug("connected hence sending {} for {}", Command.SEEK, address);
    }

    @Override
    @SuppressWarnings("SynchronizeOnNonFinalField") /* context is virtually final for a given operator */

    public void disconnected()
    {
      connected = false;
      ConnectionStatus connectionStatus = new ConnectionStatus();
      connectionStatus.connected = false;
      connectionStatus.spec = connectionSpecs[0];
      synchronized (context) {
        context.setCustomStats(connectionStatus);
      }
      super.disconnected();
    }

  }

  public static class StatsListner extends ZKAssistedDiscovery implements com.datatorrent.api.StatsListener, Serializable
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
    private final transient HashMap<Integer, ConnectionStatus> map;
    private transient long nextMillis;
    private final Response response;

    public StatsListner()
    {
      map = localMap.get();
      nextMillis = System.currentTimeMillis() + intervalMillis;
      response = new Response();
    }

    @Override
    public Response processStats(BatchedOperatorStats stats)
    {
      response.repartitionRequired = false;

      CustomStats lastStat = null;
      List<OperatorStats> lastWindowedStats = stats.getLastWindowedStats();
      for (OperatorStats os: lastWindowedStats) {
        if (os.customStats != null) {
          lastStat = os.customStats;
        }
      }

      if (lastStat instanceof ConnectionStatus) {
        ConnectionStatus cs = (ConnectionStatus)lastStat;
        map.put(stats.getOperatorId(), cs);
        if (!cs.connected) {
          response.repartitionRequired = true;
        }
      }

      if (System.currentTimeMillis() >= nextMillis) {
        Collection<Service<byte[]>> addresses = discover();
        if (addresses.size() != map.size()) {
          AbstractFlumeInputOperator.addresses.set(addresses);
          response.repartitionRequired = true;
        }
        nextMillis = System.currentTimeMillis() + intervalMillis;
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

    long intervalMillis = 60 * 1000L;
    private static final long serialVersionUID = 201312241646L;
  }

  public static class ConnectionStatus implements CustomStats
  {
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

    private static final long serialVersionUID = 201312261615L;
  }

  private static final transient ThreadLocal<HashMap<Integer, ConnectionStatus>> localMap = new ThreadLocal<HashMap<Integer, ConnectionStatus>>()
  {
    @Override
    protected HashMap<Integer, ConnectionStatus> initialValue()
    {
      return new HashMap<Integer, ConnectionStatus>();
    }

  };

  private static final transient ThreadLocal<Collection<Service<byte[]>>> addresses = new ThreadLocal<Collection<Service<byte[]>>>();
  private static final Logger logger = LoggerFactory.getLogger(AbstractFlumeInputOperator.class);
}
