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
package org.apache.apex.malhar.flume.sink;

import java.io.IOError;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ServiceConfigurationError;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.flume.discovery.Discovery;
import org.apache.apex.malhar.flume.sink.Server.Client;
import org.apache.apex.malhar.flume.sink.Server.Request;
import org.apache.apex.malhar.flume.storage.EventCodec;
import org.apache.apex.malhar.flume.storage.Storage;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import com.datatorrent.api.Component;
import com.datatorrent.api.StreamCodec;

import com.datatorrent.netlet.DefaultEventLoop;
import com.datatorrent.netlet.NetletThrowable;
import com.datatorrent.netlet.NetletThrowable.NetletRuntimeException;
import com.datatorrent.netlet.util.Slice;

/**
 * FlumeSink is a flume sink developed to ingest the data into DataTorrent DAG
 * from flume. It's essentially a flume sink which acts as a server capable of
 * talking to one client at a time. The client for this server is AbstractFlumeInputOperator.
 * <p />
 * &lt;experimental&gt;FlumeSink auto adjusts the rate at which it consumes the data from channel to
 * match the throughput of the DAG.&lt;/experimental&gt;
 * <p />
 * The properties you can set on the FlumeSink are: <br />
 * id - string unique value identifying this sink <br />
 * hostname - string value indicating the fqdn or ip address of the interface on which the server should listen <br />
 * port - integer value indicating the numeric port to which the server should bind <br />
 * sleepMillis - integer value indicating the number of milliseconds the process should sleep when there are no events
 * before checking for next event again <br />
 * throughputAdjustmentPercent - integer value indicating by what percentage the flume transaction size should be
 * adjusted upward or downward at a time <br />
 * minimumEventsPerTransaction - integer value indicating the minimum number of events per transaction <br />
 * maximumEventsPerTransaction - integer value indicating the maximum number of events per transaction. This value can
 * not be more than channel's transaction capacity.<br />
 *
 * @since 0.9.2
 */
public class FlumeSink extends AbstractSink implements Configurable
{
  private static final String HOSTNAME_STRING = "hostname";
  private static final String HOSTNAME_DEFAULT = "locahost";
  private static final long ACCEPTED_TOLERANCE = 20000;
  private DefaultEventLoop eventloop;
  private Server server;
  private int outstandingEventsCount;
  private int lastConsumedEventsCount;
  private int idleCount;
  private byte[] playback;
  private Client client;
  private String hostname;
  private int port;
  private String id;
  private long acceptedTolerance;
  private long sleepMillis;
  private double throughputAdjustmentFactor;
  private int minimumEventsPerTransaction;
  private int maximumEventsPerTransaction;
  private long commitEventTimeoutMillis;
  private transient long lastCommitEventTimeMillis;
  private Storage storage;
  Discovery<byte[]> discovery;
  StreamCodec<Event> codec;
  /* Begin implementing Flume Sink interface */

  @Override
  @SuppressWarnings({"BroadCatchBlock", "TooBroadCatch", "UseSpecificCatch", "SleepWhileInLoop"})
  public Status process() throws EventDeliveryException
  {
    Slice slice;
    synchronized (server.requests) {
      for (Request r : server.requests) {
        logger.debug("found {}", r);
        switch (r.type) {
          case SEEK:
            lastCommitEventTimeMillis = System.currentTimeMillis();
            slice = r.getAddress();
            playback = storage.retrieve(Arrays.copyOfRange(slice.buffer, slice.offset, slice.offset + slice.length));
            client = r.client;
            break;

          case COMMITTED:
            lastCommitEventTimeMillis = System.currentTimeMillis();
            slice = r.getAddress();
            storage.clean(Arrays.copyOfRange(slice.buffer, slice.offset, slice.offset + slice.length));
            break;

          case CONNECTED:
            logger.debug("Connected received, ignoring it!");
            break;

          case DISCONNECTED:
            if (r.client == client) {
              client = null;
              outstandingEventsCount = 0;
            }
            break;

          case WINDOWED:
            lastConsumedEventsCount = r.getEventCount();
            idleCount = r.getIdleCount();
            outstandingEventsCount -= lastConsumedEventsCount;
            break;

          case SERVER_ERROR:
            throw new IOError(null);

          default:
            logger.debug("Cannot understand the request {}", r);
            break;
        }
      }

      server.requests.clear();
    }

    if (client == null) {
      logger.info("No client expressed interest yet to consume the events.");
      return Status.BACKOFF;
    } else if (System.currentTimeMillis() - lastCommitEventTimeMillis > commitEventTimeoutMillis) {
      logger.info("Client has not processed the workload given for the last {} milliseconds, so backing off.",
          System.currentTimeMillis() - lastCommitEventTimeMillis);
      return Status.BACKOFF;
    }

    int maxTuples;
    // the following logic needs to be fixed... this is a quick put together.
    if (outstandingEventsCount < 0) {
      if (idleCount > 1) {
        maxTuples = (int)((1 + throughputAdjustmentFactor * idleCount) * lastConsumedEventsCount);
      } else {
        maxTuples = (int)((1 + throughputAdjustmentFactor) * lastConsumedEventsCount);
      }
    } else if (outstandingEventsCount > lastConsumedEventsCount) {
      maxTuples = (int)((1 - throughputAdjustmentFactor) * lastConsumedEventsCount);
    } else {
      if (idleCount > 0) {
        maxTuples = (int)((1 + throughputAdjustmentFactor * idleCount) * lastConsumedEventsCount);
        if (maxTuples <= 0) {
          maxTuples = minimumEventsPerTransaction;
        }
      } else {
        maxTuples = lastConsumedEventsCount;
      }
    }

    if (maxTuples >= maximumEventsPerTransaction) {
      maxTuples = maximumEventsPerTransaction;
    } else if (maxTuples <= 0) {
      maxTuples = minimumEventsPerTransaction;
    }

    if (maxTuples > 0) {
      if (playback != null) {
        try {
          int i = 0;
          do {
            if (!client.write(playback)) {
              retryWrite(playback, null);
            }
            outstandingEventsCount++;
            playback = storage.retrieveNext();
          }
          while (++i < maxTuples && playback != null);
        } catch (Exception ex) {
          logger.warn("Playback Failed", ex);
          if (ex instanceof NetletThrowable) {
            try {
              eventloop.disconnect(client);
            } finally {
              client = null;
              outstandingEventsCount = 0;
            }
          }
          return Status.BACKOFF;
        }
      } else {
        int storedTuples = 0;

        Transaction t = getChannel().getTransaction();
        try {
          t.begin();

          Event e;
          while (storedTuples < maxTuples && (e = getChannel().take()) != null) {
            Slice event = codec.toByteArray(e);
            byte[] address = storage.store(event);
            if (address != null) {
              if (!client.write(address, event)) {
                retryWrite(address, event);
              }
              outstandingEventsCount++;
            } else {
              logger.debug("Detected the condition of recovery from flume crash!");
            }
            storedTuples++;
          }

          if (storedTuples > 0) {
            storage.flush();
          }

          t.commit();

          if (storedTuples > 0) { /* log less frequently */
            logger.debug("Transaction details maxTuples = {}, storedTuples = {}, outstanding = {}",
                maxTuples, storedTuples, outstandingEventsCount);
          }
        } catch (Error er) {
          t.rollback();
          throw er;
        } catch (Exception ex) {
          logger.error("Transaction Failed", ex);
          if (ex instanceof NetletRuntimeException && client != null) {
            try {
              eventloop.disconnect(client);
            } finally {
              client = null;
              outstandingEventsCount = 0;
            }
          }
          t.rollback();
          return Status.BACKOFF;
        } finally {
          t.close();
        }

        if (storedTuples == 0) {
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
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void start()
  {
    try {
      if (storage instanceof Component) {
        @SuppressWarnings("unchecked")
        Component<com.datatorrent.api.Context> component = (Component<com.datatorrent.api.Context>)storage;
        component.setup(null);
      }
      if (discovery instanceof Component) {
        @SuppressWarnings("unchecked")
        Component<com.datatorrent.api.Context> component = (Component<com.datatorrent.api.Context>)discovery;
        component.setup(null);
      }
      if (codec instanceof Component) {
        @SuppressWarnings("unchecked")
        Component<com.datatorrent.api.Context> component = (Component<com.datatorrent.api.Context>)codec;
        component.setup(null);
      }
      eventloop = new DefaultEventLoop("EventLoop-" + id);
      server = new Server(id, discovery,acceptedTolerance);
    } catch (Error error) {
      throw error;
    } catch (RuntimeException re) {
      throw re;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    eventloop.start();
    eventloop.start(hostname, port, server);
    super.start();
  }

  @Override
  public void stop()
  {
    try {
      super.stop();
    } finally {
      try {
        if (client != null) {
          eventloop.disconnect(client);
          client = null;
        }

        eventloop.stop(server);
        eventloop.stop();

        if (codec instanceof Component) {
          @SuppressWarnings("unchecked")
          Component<com.datatorrent.api.Context> component = (Component<com.datatorrent.api.Context>)codec;
          component.teardown();
        }
        if (discovery instanceof Component) {
          @SuppressWarnings("unchecked")
          Component<com.datatorrent.api.Context> component = (Component<com.datatorrent.api.Context>)discovery;
          component.teardown();
        }
        if (storage instanceof Component) {
          @SuppressWarnings("unchecked")
          Component<com.datatorrent.api.Context> component = (Component<com.datatorrent.api.Context>)storage;
          component.teardown();
        }
      } catch (Throwable cause) {
        throw new ServiceConfigurationError("Failed Stop", cause);
      }
    }
  }

  /* End implementing Flume Sink interface */

  /* Begin Configurable Interface */
  @Override
  public void configure(Context context)
  {
    hostname = context.getString(HOSTNAME_STRING, HOSTNAME_DEFAULT);
    port = context.getInteger("port", 0);
    id = context.getString("id");
    if (id == null) {
      id = getName();
    }
    acceptedTolerance = context.getLong("acceptedTolerance", ACCEPTED_TOLERANCE);
    sleepMillis = context.getLong("sleepMillis", 5L);
    throughputAdjustmentFactor = context.getInteger("throughputAdjustmentPercent", 5) / 100.0;
    maximumEventsPerTransaction = context.getInteger("maximumEventsPerTransaction", 10000);
    minimumEventsPerTransaction = context.getInteger("minimumEventsPerTransaction", 100);
    commitEventTimeoutMillis = context.getLong("commitEventTimeoutMillis", Long.MAX_VALUE);

    @SuppressWarnings("unchecked")
    Discovery<byte[]> ldiscovery = configure("discovery", Discovery.class, context);
    if (ldiscovery == null) {
      logger.warn("Discovery agent not configured for the sink!");
      discovery = new Discovery<byte[]>()
      {
        @Override
        public void unadvertise(Service<byte[]> service)
        {
          logger.debug("Sink {} stopped listening on {}:{}", service.getId(), service.getHost(), service.getPort());
        }

        @Override
        public void advertise(Service<byte[]> service)
        {
          logger.debug("Sink {} started listening on {}:{}", service.getId(), service.getHost(), service.getPort());
        }

        @Override
        @SuppressWarnings("unchecked")
        public Collection<Service<byte[]>> discover()
        {
          return Collections.EMPTY_SET;
        }

      };
    } else {
      discovery = ldiscovery;
    }

    storage = configure("storage", Storage.class, context);
    if (storage == null) {
      logger.warn("storage key missing... FlumeSink may lose data!");
      storage = new Storage()
      {
        @Override
        public byte[] store(Slice slice)
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

      };
    }

    @SuppressWarnings("unchecked")
    StreamCodec<Event> lCodec = configure("codec", StreamCodec.class, context);
    if (lCodec == null) {
      codec = new EventCodec();
    } else {
      codec = lCodec;
    }

  }

  /* End Configurable Interface */

  @SuppressWarnings({"UseSpecificCatch", "BroadCatchBlock", "TooBroadCatch"})
  private static <T> T configure(String key, Class<T> clazz, Context context)
  {
    String classname = context.getString(key);
    if (classname == null) {
      return null;
    }

    try {
      Class<?> loadClass = Thread.currentThread().getContextClassLoader().loadClass(classname);
      if (clazz.isAssignableFrom(loadClass)) {
        @SuppressWarnings("unchecked")
        T object = (T)loadClass.newInstance();
        if (object instanceof Configurable) {
          Context context1 = new Context(context.getSubProperties(key + '.'));
          String id = context1.getString(Storage.ID);
          if (id == null) {
            id = context.getString(Storage.ID);
            logger.debug("{} inherited id={} from sink", key, id);
            context1.put(Storage.ID, id);
          }
          ((Configurable)object).configure(context1);
        }

        return object;
      } else {
        logger.error("key class {} does not implement {} interface", classname, Storage.class.getCanonicalName());
        throw new Error("Invalid storage " + classname);
      }
    } catch (Error error) {
      throw error;
    } catch (RuntimeException re) {
      throw re;
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

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

  public long getAcceptedTolerance()
  {
    return acceptedTolerance;
  }

  public void setAcceptedTolerance(long acceptedTolerance)
  {
    this.acceptedTolerance = acceptedTolerance;
  }

  /**
   * @param port the port to set
   */
  void setPort(int port)
  {
    this.port = port;
  }

  /**
   * @return the discovery
   */
  Discovery<byte[]> getDiscovery()
  {
    return discovery;
  }

  /**
   * @param discovery the discovery to set
   */
  void setDiscovery(Discovery<byte[]> discovery)
  {
    this.discovery = discovery;
  }

  /**
   * Attempt the sequence of writing after sleeping twice and upon failure assume
   * that the client connection has problems and hence close it.
   *
   * @param address
   * @param e
   * @throws IOException
   */
  private void retryWrite(byte[] address, Slice event) throws IOException
  {
    if (event == null) {  /* this happens for playback where address and event are sent as single object */
      while (client.isConnected()) {
        sleep();
        if (client.write(address)) {
          return;
        }
      }
    } else {  /* this happens when the events are taken from the flume channel and writing first time failed */
      while (client.isConnected()) {
        sleep();
        if (client.write(address, event)) {
          return;
        }
      }
    }

    throw new IOException("Client disconnected!");
  }

  private static final Logger logger = LoggerFactory.getLogger(FlumeSink.class);
}
