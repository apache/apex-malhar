/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.redis;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.ActivationListener;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.ShipContainingJars;
import com.lambdaworks.redis.KeyValue;
import com.lambdaworks.redis.RedisAsyncConnection;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.codec.RedisCodec;

/**
 * Redis input operator base.
 * 
 * @since 0.3.5
 */
@ShipContainingJars(classes = { RedisClient.class })
public abstract class AbstractRedisListInputOperator<K, V> extends BaseOperator implements InputOperator, ActivationListener<OperatorContext>
{

  /**
   * Adds ability to use various read strategies from a Redis list, such as
   * blpop, brpop, lpop, rpop, etc.
   * 
   * @param <K>  Redis list type (String, byte[], etc)
   * @param <V>  Returned value from Redis list
   */
  public interface RedisReadStrategy<K, V>
  {
    V read(RedisAsyncConnection<K, V> conn, K[] keys, int timeoutMS);
  }

  /**
   * Implements BLPOP read strategy 
   */
  public static class RedisBLPOP<K, V> implements RedisReadStrategy<K, V>
  {
    public V read(RedisAsyncConnection<K, V> conn, K[] keys, int timeoutMS)
    {
      V message = null;
      KeyValue<K,V> kv = null;
      try {
        kv = conn.blpop(timeoutMS, keys).get();
        if (kv != null) message = kv.value;
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
      return message;
    }
  }

  /**
   * Implements BRPOP read strategy 
   */
  public static class RedisBRPOP<K, V> implements RedisReadStrategy<K, V>
  {
    public V read(RedisAsyncConnection<K, V> conn, K[] keys, int timeoutMS)
    {
      V message = null;
      KeyValue<K,V> kv = null;
      try {
        kv = conn.brpop(timeoutMS, keys).get();
        if (kv != null) message = kv.value;
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
      return message;
    }
  }

  /**
   *  Allows switching of various codec types depending on output type needed (Strings, byte[], etc)
   */
  public abstract RedisCodec<K, V> getCodec();
  public abstract RedisReadStrategy<K, V> getReadStrategy();

  protected transient RedisClient redisClient;
  protected transient RedisAsyncConnection<K, V> redisConnection;
  private String host = "localhost";
  private int port = 6379;
  private int dbIndex = 0;
  private int timeoutMS = 0;
  @NotNull
  private K[] redisKeys;
  private static final int DEFAULT_BLAST_SIZE = 1000;
  private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
  private int emitBatchSize = DEFAULT_BLAST_SIZE;
  private int bufferSize = DEFAULT_BUFFER_SIZE;
  private RedisReadStrategy<K, V> redisReadStrategy = getReadStrategy();
  private volatile boolean running = false;
  private transient ArrayBlockingQueue<V> holdingBuffer = new ArrayBlockingQueue<V>(bufferSize);
  

  public RedisClient getRedisClient()
  {
    return redisClient;
  }

  public RedisAsyncConnection<K, V> getRedisConnection()
  {
    return redisConnection;
  }

  public String getHost()
  {
    return host;
  }

  public int getPort()
  {
    return port;
  }

  public int getDbIndex()
  {
    return dbIndex;
  }

  public int getTimeoutMS()
  {
    return timeoutMS;
  }

  public K[] getRedisKeys()
  {
    return redisKeys;
  }

  public RedisReadStrategy<K, V> getRedisReadStrategy()
  {
    return redisReadStrategy;
  }

  public void setRedisReadStrategy(RedisReadStrategy<K, V> rrs)
  {
    this.redisReadStrategy = rrs;
  }

  public void setHost(String host)
  {
    this.host = host;
  }

  public void setPort(int port)
  {
    this.port = port;
  }

  public void setDbIndex(int index)
  {
    this.dbIndex = index;
  }

  public void setTimeoutMS(int timeout)
  {
    this.timeoutMS = timeout;
  }

  public void setRedisKeys(K... redisKeys)
  {
    this.redisKeys = redisKeys;
  }

  // @Min(1)
  public void setTupleBlast(int i)
  {
    this.emitBatchSize = i;
  }

  public void setBufferSize(int size)
  {
    this.bufferSize = size;
  }

  @Override
  public void setup(OperatorContext ctx)
  {
    redisClient = new RedisClient(host, port);
    redisConnection = redisClient.connectAsync(this.getCodec());
    redisConnection.select(dbIndex);
    redisConnection.setTimeout(timeoutMS, TimeUnit.MILLISECONDS);
  }

  @Override
  public void teardown()
  {
    redisConnection.close();
    redisClient.shutdown();
  }

  /**
   * Start a thread receiving data from Redis and place it into holdingBuffer
   * 
   */
  @Override
  public void activate(OperatorContext ctx)
  {
    running = true;
    new Thread() {
      @Override
      public void run()
      {
        while (running) {
          try {
            V message = redisReadStrategy.read(redisConnection, redisKeys, timeoutMS);
            if (message != null) {
              holdingBuffer.add(message);
            }
          } catch (Exception e) {
            logger.error("Failed to read data from Redis on list " + redisKeys.toString(), e);
            running = false;
          }
        }
      }
    }.start();
  }

  @Override
  public void deactivate()
  {
    running = false;
  }

  public abstract void emitTuple(V message);

  @Override
  public void emitTuples()
  {
    int ntuples = emitBatchSize;
    if (ntuples > holdingBuffer.size()) {
      ntuples = holdingBuffer.size();
      if (ntuples == 0 && !running) {
        throw new RuntimeException("No longer reading from Redis");
      }
    }
    for (int i = ntuples; i-- > 0;) {
      emitTuple(holdingBuffer.poll());
    }
  }

  public static final Logger logger = LoggerFactory.getLogger(AbstractRedisListInputOperator.class);
}
