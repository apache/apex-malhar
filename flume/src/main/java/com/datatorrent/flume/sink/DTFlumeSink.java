/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.sink;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import com.datatorrent.flume.sink.Server.Request;
import com.datatorrent.flume.storage.RetrievalObject;
import com.datatorrent.flume.storage.Storage;
import com.datatorrent.netlet.DefaultEventLoop;

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
  private boolean playback;
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
  public Status process() throws EventDeliveryException
  {
    synchronized (server.requests) {
      for (Request r : server.requests) {
        logger.debug("found {}", r);
        switch (r.type) {
          case SEEK:
            playback = storage.retrieve(r.getAddress()) != null;
            process = true;
            break;

          case COMMITTED:
            storage.clean(r.getAddress());
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
      if (playback) {
        logger.debug("playback mode still active");

        RetrievalObject next;
        int i = 0;
        while (i < maxTuples && (next = storage.retrieveNext()) != null) {
          server.client.write(next.getToken(), next.getData());
          i++;
        }

        if (i == 0) {
          playback = false;
        }
        else {
          outstandingEventsCount += i;
        }
      }
      else {
        Transaction t = getChannel().getTransaction();
        try {
          t.begin();

          Event e;
          int i = 0;
          while (i < maxTuples && (e = getChannel().take()) != null) {
            long l = storage.store(e.getBody());
            server.client.write(l, e.getBody());
            i++;
          }

          if (i == 0) {
            sleep();
          }
          else {
            outstandingEventsCount += i;
            storage.flush();
            logger.debug("Transaction details maxTuples = {}, i = {}, outstanding = {}", maxTuples, i, outstandingEventsCount);
          }

          t.commit();
        }
        catch (Throwable ex) {
          logger.error("Exception during flume transaction", ex);
          t.rollback();

          if (ex instanceof Error) {
            throw (Error)ex;
          }

          return Status.BACKOFF;
        }
        finally {
          t.close();
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
    logger.debug("starting server...");
    try {
      eventloop = new DefaultEventLoop(eventloopName == null ? "EventLoop-" + getName() : eventloopName);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    eventloop.start();
    eventloop.start(hostname, port, server);
    logger.debug("started server!");
    super.start();
  }

  @Override
  public void stop()
  {
    logger.debug("stopping server...");
    try {
      super.stop();
    }
    finally {
      eventloop.stop(server);
      eventloop.stop();
    }
    logger.debug("stopped server!");
  }

  /* End implementing Flume Sink interface */

  /* Begin Configurable Interface */
  @Override
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
        public long store(byte[] bytes)
        {
          return 0;
        }

        @Override
        public RetrievalObject retrieve(long identifier)
        {
          return null;
        }

        @Override
        public RetrievalObject retrieveNext()
        {
          return null;
        }

        @Override
        public boolean clean(long identifier)
        {
          return true;
        }

        @Override
        public boolean flush()
        {
          return true;
        }
        
        @Override
        public boolean close()
        {
          return true;
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

  private static final Logger logger = LoggerFactory.getLogger(DTFlumeSink.class);
}
