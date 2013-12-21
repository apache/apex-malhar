/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.sink;

import com.datatorrent.common.util.Slice;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import com.datatorrent.flume.sink.Server.Request;
import com.datatorrent.flume.storage.Storage;
import com.datatorrent.netlet.DefaultEventLoop;
import java.util.Arrays;

/**
 * DTFlumeSink is a flume sink developed to ingest the data into DataTorrent DAG
 * from flume. It's essentially a flume sink which acts as a server capable of
 * talking to one client at a time. The client for this server is AbstractFlumeInputOperator.
 * <p />
 * &lt;experimental&gt;DTFlumeSink auto adjusts the rate at which it consumes the data from channel to
 * match the throughput of the DAG.&lt;/experimental&gt;
 * <p />
 * The properties you can set on the DTFlumeSink are: <br />
 * hostname - string value indicating the fqdn or ip address of the interface on which the server should listen <br />
 * port - integer value indicating the numeric port to which the server should bind <br />
 * eventloopName - string value indicating the name of the network io thread <br />
 * sleepMillis - integer value indicating the number of milliseconds the process should sleep when there are no events before checking for next event again <br />
 * throughputAdjustmentPercent - integer value indicating by what percentage the flume transaction size should be adjusted upward or downward at a time <br />
 * minimumEventsPerTransaction - integer value indicating the minimum number of events per transaction <br />
 * maximumEventsPerTransaction - integer value indicating the maximum number of events per transaction. This value can not be more than channel's transaction capacity.<br />
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class DTFlumeSink extends AbstractSink implements Configurable
{
  private DefaultEventLoop eventloop;
  private Server server;
  private int outstandingEventsCount;
  private int lastConsumedEventsCount;
  private int idleCount;
  private byte[] playback;
  private boolean process;
  private Storage storage;
  private String hostname;
  private int port;
  private String eventloopName;
  private long sleepMillis;
  private double throughputAdjustmentFactor;
  private int minimumEventsPerTransaction;
  private int maximumEventsPerTransaction;

  public DTFlumeSink()
  {
    try {
      server = new Server();
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /* Begin implementing Flume Sink interface */
  @Override
  @SuppressWarnings({"BroadCatchBlock", "TooBroadCatch"})
  public Status process() throws EventDeliveryException
  {
    Slice slice;
    synchronized (server.requests) {
      for (Request r: server.requests) {
        logger.debug("found {}", r);
        switch (r.type) {
          case SEEK:
            slice = r.getAddress();
            playback = storage.retrieve(Arrays.copyOfRange(slice.buffer, slice.offset, slice.offset + slice.length));
            process = true;
            break;

          case COMMITTED:
            slice = r.getAddress();
            storage.clean(Arrays.copyOfRange(slice.buffer, slice.offset, slice.offset + slice.length));
            break;

          case CONNECTED:
          case DISCONNECTED:
            process = false;
            break;

          case WINDOWED:
            lastConsumedEventsCount = r.getEventCount();
            idleCount = r.getIdleCount();
            outstandingEventsCount -= lastConsumedEventsCount;
            break;

          default:
            logger.debug("Cannot understand the request {}", r);
            break;
        }
      }

      server.requests.clear();
    }

    if (!process) {
      logger.debug("No client expressed interest yet to consume the events");
      return Status.BACKOFF;
    }

    int maxTuples;
    // the following logic needs to be fixed... this is a quick put together.
    if (outstandingEventsCount < 0) {
      if (idleCount > 1) {
        maxTuples = (int)((1 + throughputAdjustmentFactor * idleCount) * lastConsumedEventsCount);
      }
      else {
        maxTuples = (int)((1 + throughputAdjustmentFactor) * lastConsumedEventsCount);
      }
    }
    else if (outstandingEventsCount > lastConsumedEventsCount) {
      maxTuples = (int)((1 - throughputAdjustmentFactor) * lastConsumedEventsCount);
    }
    else {
      if (idleCount > 0) {
        maxTuples = (int)((1 + throughputAdjustmentFactor * idleCount) * lastConsumedEventsCount);
        if (maxTuples <= 0) {
          maxTuples = minimumEventsPerTransaction;
        }
      }
      else {
        maxTuples = lastConsumedEventsCount;
      }
    }

    if (maxTuples >= maximumEventsPerTransaction) {
      maxTuples = maximumEventsPerTransaction;
    }

    if (maxTuples > 0) {
      if (playback != null) {
        logger.debug("playback mode is active.");
        int i = 0;
        do {
          server.client.write(playback);
          playback = storage.retrieveNext();
        }
        while (++i < maxTuples && playback != null);

        outstandingEventsCount += i;
      }
      else {
        int i = 0;

        Transaction t = getChannel().getTransaction();
        try {
          t.begin();

          Event e;
          while (i < maxTuples && (e = getChannel().take()) != null) {
            byte[] address = storage.store(e.getBody());
            if (address != null) {
              server.client.write(address, e.getBody());
            }
            else {
              logger.debug("Detected the condition of recovery from flume crash!");
            }
            i++;
          }

          if (i > 0) {
            outstandingEventsCount += i;
            storage.flush();
            logger.debug("Transaction details maxTuples = {}, i = {}, outstanding = {}", maxTuples, i, outstandingEventsCount);
          }

          t.commit();
        }
        catch (Throwable th) {
          logger.error("Exception during flume transaction", th);
          t.rollback();

          if (th instanceof Error) {
            throw (Error)th;
          }

          return Status.BACKOFF;
        }
        finally {
          t.close();
        }

        if (i == 0) {
          sleep();
        }
      }
    }

    return Status.READY;
  }

  private void sleep()
  {
    try {
      Thread.sleep(sleepMillis);
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void start()
  {
    logger.debug("Starting server binding at {}:{}", hostname, port);
    try {
      eventloop = new DefaultEventLoop(eventloopName == null ? "EventLoop-" + getName() : eventloopName);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    eventloop.start();
    eventloop.start(hostname, port, server);
    logger.info("Server listening at {}:{}.", hostname, port);
    super.start();
  }

  @Override
  public void stop()
  {
    logger.debug("Stopping server at {}:{}", hostname, port);
    try {
      super.stop();
    }
    finally {
      eventloop.stop(server);
      eventloop.stop();
    }
    logger.info("Stopped server at {}:{}", hostname, port);
  }

  /* End implementing Flume Sink interface */

  /* Begin Configurable Interface */
  @Override
  @SuppressWarnings({"BroadCatchBlock", "TooBroadCatch", "UseSpecificCatch"})
  public void configure(Context context)
  {
    hostname = context.getString("hostname", "localhost");
    port = context.getInteger("port", 5033);
    eventloopName = context.getString("eventloopName");
    sleepMillis = context.getLong("sleepMillis", 5L);
    throughputAdjustmentFactor = context.getInteger("throughputAdjustmentPercent", 5) / 100.0;
    maximumEventsPerTransaction = context.getInteger("maximumEventsPerTransaction", 10000);
    minimumEventsPerTransaction = context.getInteger("minimumEventsPerTransaction", 100);

    logger.debug("hostname = {}\nport = {}\neventloopName = {}\nsleepMillis = {}\nthroughputAdjustmentFactor = {}\nmaximumEventsPerTransaction = {}\nminimumEventsPerTransaction = {}", hostname, port, eventloopName, sleepMillis, throughputAdjustmentFactor, maximumEventsPerTransaction, minimumEventsPerTransaction);

    String lStorage = context.getString("storage");
    if (lStorage == null) {
      logger.warn("storage key missing... DTFlumeSink may lose data!");
      storage = new Storage()
      {
        @Override
        public byte[] store(byte[] bytes)
        {
          return null;
        }

        @Override
        public byte[] retrieve(byte[] identifier)
        {
          return null;
        }

        @Override
        public byte[] retrieveNext()
        {
          return null;
        }

        @Override
        public void clean(byte[] identifier)
        {

        }

        @Override
        public void flush()
        {
        }

        @Override
        public void close()
        {

        }

      };
    }
    else {
      try {
        Class<?> loadClass = Thread.currentThread().getContextClassLoader().loadClass(lStorage);
        if (Storage.class.isAssignableFrom(loadClass)) {
          storage = (Storage)loadClass.newInstance();
          if (storage instanceof Configurable) {
            ((Configurable)storage).configure(new Context(context.getSubProperties("storage.")));
          }
        }
        else {
          logger.error("storage class {} does not implement {} interface", lStorage, Storage.class.getCanonicalName());
          throw new Error("Invalid storage " + lStorage);
        }
      }
      catch (Throwable t) {
        if (t instanceof RuntimeException) {
          throw (RuntimeException)t;
        }
        else {
          throw new RuntimeException(t);
        }
      }
    }
  }
  /* End Configurable Interface */

  /**
   * @return the hostname
   */
  String getHostname()
  {
    return hostname;
  }

  /**
   * @param hostname the hostname to set
   */
  void setHostname(String hostname)
  {
    this.hostname = hostname;
  }

  /**
   * @return the port
   */
  int getPort()
  {
    return port;
  }

  /**
   * @param port the port to set
   */
  void setPort(int port)
  {
    this.port = port;
  }

  private static final Logger logger = LoggerFactory.getLogger(DTFlumeSink.class);
}
