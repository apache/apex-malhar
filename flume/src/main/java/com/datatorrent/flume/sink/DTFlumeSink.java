/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.sink;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flume.*;
import org.apache.flume.lifecycle.LifecycleState;

import com.datatorrent.flume.sink.Server.Request;
import com.datatorrent.netlet.DefaultEventLoop;
import com.datatorrent.storage.RetrievalObject;
import com.datatorrent.storage.Storage;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class DTFlumeSink implements Sink
{
  public static final double THROUGHPUT_ADJUSTMENT_FACTOR = 0.05;
  public static final int MIN_TUPLES_COUNT = 100;
  public static final int MAX_TUPLES_COUNT = 10000;
  private Channel channel;
  private String name;
  private LifecycleState state;
  private DefaultEventLoop eventloop;
  private Server server;
  private int outstandingEventsCount;
  private int lastConsumedEventsCount;
  private int idleCount;
  private boolean playback;
  Storage storage = new Storage()
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

  };

  public DTFlumeSink()
  {
    state = LifecycleState.ERROR;
    try {
      server = new Server();
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /* Begin implementing Flume Sink interface */
  @Override
  public void setChannel(Channel chnl)
  {
    channel = chnl;
  }

  @Override
  public Channel getChannel()
  {
    return channel;
  }

  @Override
  public Status process() throws EventDeliveryException
  {
    synchronized (server.requests) {
      for (Request r : server.requests) {
        logger.debug("found {}", r);
        switch (r.type) {
          case SEEK:
            playback = storage.retrieve(r.getAddress()) != null;
            state = LifecycleState.IDLE;
            break;

          case COMMITTED:
            storage.clean(r.getAddress());
            break;

          case DISCONNECTED:
            state = LifecycleState.ERROR;
            break;

          case CONNECTED:
            state = LifecycleState.ERROR;
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

    if (state != LifecycleState.IDLE) {
      logger.debug("returning backoff since state = {}", state);
      return Status.BACKOFF;
    }

    if (playback) {
      logger.debug("playback mode still active");
      RetrievalObject next;
      while ((next = storage.retrieveNext()) != null) {
        server.client.write(next.getToken(), next.getData());
      }
      playback = false;
    }
    else {
      int maxTuples;
      // the following logic needs to be fixed... this is a quick put together.
      if (outstandingEventsCount < 0) {
        if (idleCount > 1) {
          maxTuples = (int)((1 + THROUGHPUT_ADJUSTMENT_FACTOR * idleCount) * lastConsumedEventsCount);
        }
        else {
          maxTuples = (int)((1 + THROUGHPUT_ADJUSTMENT_FACTOR) * lastConsumedEventsCount);
        }
      }
      else if (outstandingEventsCount > lastConsumedEventsCount) {
        maxTuples = (int)((1 - THROUGHPUT_ADJUSTMENT_FACTOR) * lastConsumedEventsCount);
      }
      else {
        if (idleCount > 0) {
          maxTuples = (int)((1 + THROUGHPUT_ADJUSTMENT_FACTOR * idleCount) * lastConsumedEventsCount);
          if (maxTuples <= 0) {
            maxTuples = MIN_TUPLES_COUNT;
          }
        }
        else {
          maxTuples = lastConsumedEventsCount;
        }
      }

      if (maxTuples >= MAX_TUPLES_COUNT) {
        maxTuples = MAX_TUPLES_COUNT;
      }

      if (maxTuples > 0) {
        Transaction t = channel.getTransaction();
        try {
          t.begin();

          Event e;
          int i = 0;
          while (i < maxTuples && (e = channel.take()) != null) {
            long l = storage.store(e.getBody());
            server.client.write(l, e.getBody());
            i++;
          }

          outstandingEventsCount += i;
          logger.debug("Transaction details maxTuples = {}, i = {}, outstanding = {}", maxTuples, i, outstandingEventsCount);

          storage.flush();
          t.commit();
        }
        catch (Exception ex) {
          logger.error("Exception during flume transaction", ex);
          t.rollback();
          return Status.BACKOFF;
        }
        finally {
          t.close();
        }
      }
      else {
        return Status.BACKOFF;
      }
    }

    return Status.READY;
  }

  @Override
  public void start()
  {
    logger.debug("starting server...");
    try {
      eventloop = new DefaultEventLoop("EventLoop-" + getName());
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    eventloop.start();
    eventloop.start("localhost", 5033, server);
    state = LifecycleState.START;
    logger.debug("started server!");
  }

  @Override
  public void stop()
  {
    logger.debug("stopping server...");
    try {
      state = LifecycleState.STOP;
    }
    finally {
      eventloop.stop(server);
      eventloop.stop();
    }
    logger.debug("stopped server!");
  }

  @Override
  public LifecycleState getLifecycleState()
  {
    return state;
  }

  @Override
  public void setName(String string)
  {
    name = string;
  }

  @Override
  public String getName()
  {
    return name;
  }

  /* End implementing Flume Sink interface */
  private static final Logger logger = LoggerFactory.getLogger(DTFlumeSink.class);
}
