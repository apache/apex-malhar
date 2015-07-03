/*
 * Copyright (c) 2015 DataTorrent, Inc.
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
package com.datatorrent.lib.appdata.query;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import javax.validation.constraints.NotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.appdata.query.serde.MessageSerializerFactory;
import com.datatorrent.lib.appdata.schemas.Result;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.IdleTimeHandler;

import com.datatorrent.common.util.NameableThreadFactory;

public class QueryManagerAsynchronous<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, RESULT extends Result> implements Component<OperatorContext>, IdleTimeHandler
{
  private DefaultOutputPort<String> resultPort = null;

  private transient final Semaphore inWindowSemaphore = new Semaphore(0);
  private final ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<String>();
  private QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queueManager;
  private QueryExecutor<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, RESULT> queryExecutor;
  private MessageSerializerFactory messageSerializerFactory;

  @VisibleForTesting
  protected transient ExecutorService processingThread;
  private transient Thread mainThread;

  public QueryManagerAsynchronous(DefaultOutputPort<String> resultPort,
                                  QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queueManager,
                                  QueryExecutor<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, RESULT> queryExecutor,
                                  MessageSerializerFactory messageSerializerFactory,
                                  Thread mainThread)
  {
    setResultPort(resultPort);
    setQueueManager(queueManager);
    setQueryExecutor(queryExecutor);
    setMessageSerializerFactory(messageSerializerFactory);
    setMainThread(mainThread);
  }

  private void setMainThread(@NotNull Thread mainThread)
  {
    this.mainThread = Preconditions.checkNotNull(mainThread);
  }

  private void setResultPort(DefaultOutputPort<String> resultPort)
  {
    this.resultPort = Preconditions.checkNotNull(resultPort);
  }

  private void setQueueManager(QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queueManager)
  {
    this.queueManager = Preconditions.checkNotNull(queueManager);
  }

  public QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> getQueueManager()
  {
    return queueManager;
  }

  private void setQueryExecutor(QueryExecutor<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, RESULT> queryExecutor)
  {
    this.queryExecutor = Preconditions.checkNotNull(queryExecutor);
  }

  public QueryExecutor<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, RESULT> getQueryExecutor()
  {
    return queryExecutor;
  }

  private void setMessageSerializerFactory(MessageSerializerFactory messageSerializerFactory)
  {
    this.messageSerializerFactory = Preconditions.checkNotNull(messageSerializerFactory);
  }

  @Override
  public void setup(OperatorContext context)
  {
    processingThread = Executors.newSingleThreadScheduledExecutor(new NameableThreadFactory("Query Executor Thread"));
    processingThread.submit(new ProcessingThread(mainThread));
  }

  public void beginWindow(long windowID)
  {
    inWindowSemaphore.release();
    queueManager.resumeEnqueue();
  }

  @SuppressWarnings("CallToThreadYield")
  public void endWindow()
  {
    queueManager.haltEnqueue();

    while(queueManager.getNumLeft() > 0) {
      if(queue.isEmpty()) {
        Thread.yield();
      }
      else {
        emptyQueue();
      }
    }

    emptyQueue();

    try {
      inWindowSemaphore.acquire();
    }
    catch(InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }

  private void emptyQueue()
  {
    while(!queue.isEmpty()) {
      resultPort.emit(queue.poll());
    }
  }

  @Override
  @SuppressWarnings({"CallToThreadStopSuspendOrResumeManager", "deprecation"})
  public void teardown()
  {
    processingThread.shutdownNow();
  }

  @Override
  public void handleIdleTime()
  {
    emptyQueue();
  }

  private class ProcessingThread implements Callable<Void>
  {
    private Thread mainThread;

    public ProcessingThread(Thread mainThread)
    {
      setMainThread(mainThread);
    }

    private void setMainThread(Thread mainThread)
    {
      this.mainThread = Preconditions.checkNotNull(mainThread);
    }

    @Override
    public Void call() throws Exception
    {
      try {
        loop();
      }
      catch(Exception ex) {
        LOG.error("Exception thrown while processing:", ex);
        mainThread.interrupt();

        throw ex;
      }

      return null;
    }

    private void loop()
    {
      //Do this forever
      while(true) {
        //Grab something from the queue as soon as it's available.
        QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queryBundle = queueManager.dequeueBlock();

        try {
          inWindowSemaphore.acquire();
        }
        catch(InterruptedException ex) {
          throw new RuntimeException(ex);
        }

        //We are gauranteed to be in the operator's window now.
        Result result = queryExecutor.executeQuery(queryBundle.getQuery(),
                                                   queryBundle.getMetaQuery(),
                                                   queryBundle.getQueueContext());
        if(result != null) {
          String serializedMessage = messageSerializerFactory.serialize(result);
          queue.add(serializedMessage);
        }

        //We are done processing the query allow the operator to continue to the next window if it
        //wants to

        inWindowSemaphore.release();
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(QueryManagerAsynchronous.class);
}
