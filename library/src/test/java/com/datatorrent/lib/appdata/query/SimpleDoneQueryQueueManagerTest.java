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

import com.google.common.base.Preconditions;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.mutable.MutableBoolean;

import com.datatorrent.lib.appdata.ThreadUtils.ExceptionSaverExceptionHandler;
import com.datatorrent.lib.appdata.schemas.Query;

public class SimpleDoneQueryQueueManagerTest
{
  @Test
  public void firstDoneTest()
  {
    SimpleDoneQueueManager<Query, Void> sdqqm = new SimpleDoneQueueManager<Query, Void>();

    sdqqm.setup(null);
    sdqqm.beginWindow(0);

    Query query = new MockQuery("1");
    sdqqm.enqueue(query, null, new MutableBoolean(true));

    QueryBundle<Query, Void, MutableBoolean> qb = sdqqm.dequeue();
    Assert.assertEquals("Should return back null.", null, qb);

    sdqqm.endWindow();
    sdqqm.beginWindow(1);

    qb = sdqqm.dequeue();

    Assert.assertEquals("Should return back null.", null, qb);
    qb = sdqqm.dequeue();
    Assert.assertEquals("Should return back null.", null, qb);

    sdqqm.endWindow();
    sdqqm.teardown();
  }

  @Test
  public void simpleEnqueueDequeueBlock()
  {
    SimpleDoneQueueManager<Query, Void> sdqqm = new SimpleDoneQueueManager<Query, Void>();

    sdqqm.setup(null);
    sdqqm.beginWindow(0);

    Query query = new MockQuery("1");
    sdqqm.enqueue(query, null, new MutableBoolean(false));

    Assert.assertEquals(1, sdqqm.getNumLeft());
    Assert.assertEquals(sdqqm.getNumPermits(), sdqqm.getNumLeft());

    QueryBundle<Query, Void, MutableBoolean> qb = sdqqm.dequeueBlock();

    Assert.assertEquals(0, sdqqm.getNumLeft());
    Assert.assertEquals(sdqqm.getNumPermits(), sdqqm.getNumLeft());

    Assert.assertEquals("Should return same query.", query, qb.getQuery());

    sdqqm.endWindow();
    sdqqm.teardown();
  }

  @Test
  public void simpleBlockingTest() throws Exception
  {
    SimpleDoneQueueManager<Query, Void> sdqqm = new SimpleDoneQueueManager<Query, Void>();

    sdqqm.setup(null);
    sdqqm.beginWindow(0);

    testBlocking(sdqqm);

    sdqqm.endWindow();
    sdqqm.teardown();
  }

  @Test
  public void simpleEnqueueDequeue()
  {
    SimpleDoneQueueManager<Query, Void> sdqqm = new SimpleDoneQueueManager<Query, Void>();

    sdqqm.setup(null);
    sdqqm.beginWindow(0);

    Query query = new MockQuery("1");
    sdqqm.enqueue(query, null, new MutableBoolean(false));

    QueryBundle<Query, Void, MutableBoolean> qb = sdqqm.dequeue();

    Assert.assertEquals("Should return same query.", query, qb.getQuery());
    qb = sdqqm.dequeue();
    Assert.assertEquals("Should return back null.", null, qb);

    sdqqm.endWindow();
    sdqqm.beginWindow(1);

    qb = sdqqm.dequeue();
    Assert.assertEquals("Should return same query.", query, qb.getQuery());
    qb = sdqqm.dequeue();
    Assert.assertEquals("Should return back null.", null, qb);

    sdqqm.endWindow();
    sdqqm.teardown();
  }

  @Test
  public void simpleEnqueueDequeueThenBlock() throws Exception
  {
    SimpleDoneQueueManager<Query, Void> sdqqm = new SimpleDoneQueueManager<Query, Void>();

    sdqqm.setup(null);
    sdqqm.beginWindow(0);

    Query query = new MockQuery("1");
    sdqqm.enqueue(query, null, new MutableBoolean(false));

    Assert.assertEquals(1, sdqqm.getNumLeft());
    Assert.assertEquals(sdqqm.getNumPermits(), sdqqm.getNumLeft());

    QueryBundle<Query, Void, MutableBoolean> qb = sdqqm.dequeueBlock();

    Assert.assertEquals(0, sdqqm.getNumLeft());
    Assert.assertEquals(sdqqm.getNumPermits(), sdqqm.getNumLeft());

    testBlocking(sdqqm);

    sdqqm.endWindow();
    sdqqm.teardown();
  }

  @Test
  public void simpleExpire1()
  {
    SimpleDoneQueueManager<Query, Void> sdqqm = new SimpleDoneQueueManager<Query, Void>();

    sdqqm.setup(null);
    sdqqm.beginWindow(0);

    Query query = new MockQuery("1");
    sdqqm.enqueue(query, null, new MutableBoolean(false));

    QueryBundle<Query, Void, MutableBoolean> qb = sdqqm.dequeue();

    Assert.assertEquals("Should return same query.", query, qb.getQuery());
    qb.getQueueContext().setValue(true);

    qb = sdqqm.dequeue();
    Assert.assertEquals("Should return back null.", null, qb);

    sdqqm.endWindow();
    sdqqm.beginWindow(1);

    qb = sdqqm.dequeue();
    Assert.assertEquals("Should return back null.", null, qb);

    sdqqm.endWindow();
    sdqqm.teardown();
  }

  @Test
  public void resetPermitsTest() throws Exception
  {
    SimpleDoneQueueManager<Query, Void> sdqqm = new SimpleDoneQueueManager<Query, Void>();

    sdqqm.setup(null);
    sdqqm.beginWindow(0);

    Query query = new MockQuery("1");
    sdqqm.enqueue(query, null, new MutableBoolean(false));

    Assert.assertEquals(1, sdqqm.getNumLeft());
    Assert.assertEquals(sdqqm.getNumPermits(), sdqqm.getNumLeft());

    QueryBundle<Query, Void, MutableBoolean> qb = sdqqm.dequeueBlock();

    Assert.assertEquals(0, sdqqm.getNumLeft());
    Assert.assertEquals(sdqqm.getNumPermits(), sdqqm.getNumLeft());

    sdqqm.endWindow();

    sdqqm.beginWindow(1);

    Assert.assertEquals(1, sdqqm.getNumLeft());
    Assert.assertEquals(sdqqm.getNumPermits(), sdqqm.getNumLeft());

    qb = sdqqm.dequeueBlock();

    Assert.assertEquals(0, sdqqm.getNumLeft());
    Assert.assertEquals(sdqqm.getNumPermits(), sdqqm.getNumLeft());

    testBlocking(sdqqm);

    sdqqm.endWindow();
  }

  @Test
  public void expiredTestBlocking() throws Exception
  {
    SimpleDoneQueueManager<Query, Void> sdqqm = new SimpleDoneQueueManager<Query, Void>();

    sdqqm.setup(null);
    sdqqm.beginWindow(0);

    Query query = new MockQuery("1");
    MutableBoolean queueContext = new MutableBoolean(false);
    sdqqm.enqueue(query, null, queueContext);

    Assert.assertEquals(1, sdqqm.getNumLeft());
    Assert.assertEquals(sdqqm.getNumPermits(), sdqqm.getNumLeft());
    QueryBundle<Query, Void, MutableBoolean> qb = sdqqm.dequeueBlock();
    Assert.assertEquals(0, sdqqm.getNumLeft());
    Assert.assertEquals(sdqqm.getNumPermits(), sdqqm.getNumLeft());

    sdqqm.endWindow();

    sdqqm.beginWindow(1);

    Assert.assertEquals(1, sdqqm.getNumLeft());
    Assert.assertEquals(sdqqm.getNumPermits(), sdqqm.getNumLeft());

    queueContext.setValue(true);
    testBlocking(sdqqm);

    Assert.assertEquals(0, sdqqm.getNumLeft());
    Assert.assertEquals(sdqqm.getNumPermits(), sdqqm.getNumLeft());

    sdqqm.endWindow();
    sdqqm.teardown();
  }

  @Test
  public void expiredTestBlockingExpiredFirstValidLast() throws Exception
  {
    SimpleDoneQueueManager<Query, Void> sdqqm = new SimpleDoneQueueManager<Query, Void>();

    sdqqm.setup(null);
    sdqqm.beginWindow(0);

    Query query = new MockQuery("1");
    MutableBoolean queueContext = new MutableBoolean(false);
    sdqqm.enqueue(query, null, queueContext);

    Query query1 = new MockQuery("2");
    MutableBoolean queueContext1 = new MutableBoolean(false);
    sdqqm.enqueue(query1, null, queueContext1);

    Assert.assertEquals(2, sdqqm.getNumLeft());
    Assert.assertEquals(sdqqm.getNumPermits(), sdqqm.getNumLeft());
    QueryBundle<Query, Void, MutableBoolean> qb = sdqqm.dequeueBlock();
    Assert.assertEquals(1, sdqqm.getNumLeft());
    Assert.assertEquals(sdqqm.getNumPermits(), sdqqm.getNumLeft());

    sdqqm.endWindow();

    sdqqm.beginWindow(1);

    Assert.assertEquals(2, sdqqm.getNumLeft());
    Assert.assertEquals(sdqqm.getNumPermits(), sdqqm.getNumLeft());

    queueContext.setValue(true);
    qb = sdqqm.dequeueBlock();

    Assert.assertEquals(0, sdqqm.getNumLeft());
    Assert.assertEquals(sdqqm.getNumPermits(), sdqqm.getNumLeft());

    testBlocking(sdqqm);

    sdqqm.endWindow();

    sdqqm.beginWindow(2);

    Assert.assertEquals(1, sdqqm.getNumLeft());
    Assert.assertEquals(sdqqm.getNumPermits(), sdqqm.getNumLeft());

    qb = sdqqm.dequeueBlock();

    testBlocking(sdqqm);

    sdqqm.endWindow();

    sdqqm.teardown();
  }

  @Test
  public void expiredTestBlockingValidFirstExpiredLast() throws Exception
  {
    SimpleDoneQueueManager<Query, Void> sdqqm = new SimpleDoneQueueManager<Query, Void>();

    sdqqm.setup(null);
    sdqqm.beginWindow(0);

    Query query = new MockQuery("1");
    MutableBoolean queueContext = new MutableBoolean(false);
    sdqqm.enqueue(query, null, queueContext);

    Query query1 = new MockQuery("2");
    MutableBoolean queueContext1 = new MutableBoolean(false);
    sdqqm.enqueue(query1, null, queueContext1);

    Assert.assertEquals(2, sdqqm.getNumLeft());
    Assert.assertEquals(sdqqm.getNumPermits(), sdqqm.getNumLeft());
    QueryBundle<Query, Void, MutableBoolean> qb = sdqqm.dequeueBlock();
    Assert.assertEquals(1, sdqqm.getNumLeft());
    Assert.assertEquals(sdqqm.getNumPermits(), sdqqm.getNumLeft());

    sdqqm.endWindow();

    sdqqm.beginWindow(1);

    Assert.assertEquals(2, sdqqm.getNumLeft());
    Assert.assertEquals(sdqqm.getNumPermits(), sdqqm.getNumLeft());

    queueContext1.setValue(true);
    qb = sdqqm.dequeueBlock();

    Assert.assertEquals(1, sdqqm.getNumLeft());
    Assert.assertEquals(sdqqm.getNumPermits(), sdqqm.getNumLeft());

    testBlocking(sdqqm);

    Assert.assertEquals(0, sdqqm.getNumLeft());
    Assert.assertEquals(sdqqm.getNumPermits(), sdqqm.getNumLeft());

    sdqqm.endWindow();

    sdqqm.beginWindow(2);

    Assert.assertEquals(1, sdqqm.getNumLeft());
    Assert.assertEquals(sdqqm.getNumPermits(), sdqqm.getNumLeft());

    qb = sdqqm.dequeueBlock();

    testBlocking(sdqqm);

    sdqqm.endWindow();

    sdqqm.teardown();
  }

  @Test
  public void simpleExpire1ThenBlock()
  {
    SimpleDoneQueueManager<Query, Void> sdqqm = new SimpleDoneQueueManager<Query, Void>();

    sdqqm.setup(null);
    sdqqm.beginWindow(0);

    Query query = new MockQuery("1");
    sdqqm.enqueue(query, null, new MutableBoolean(false));

    QueryBundle<Query, Void, MutableBoolean> qb = sdqqm.dequeue();

    Assert.assertEquals("Should return same query.", query, qb.getQuery());
    qb.getQueueContext().setValue(true);

    qb = sdqqm.dequeue();
    Assert.assertEquals("Should return back null.", null, qb);

    sdqqm.endWindow();
    sdqqm.beginWindow(1);

    qb = sdqqm.dequeue();
    Assert.assertEquals("Should return back null.", null, qb);

    sdqqm.endWindow();
    sdqqm.teardown();
  }

  @Test
  public void simpleExpireBlockThenUnblock() throws Exception
  {
    SimpleDoneQueueManager<Query, Void> sdqqm = new SimpleDoneQueueManager<Query, Void>();

    sdqqm.setup(null);
    sdqqm.beginWindow(0);

    Query query = new MockQuery("1");
    MutableBoolean expire = new MutableBoolean(false);
    sdqqm.enqueue(query, null, expire);

    sdqqm.endWindow();
    sdqqm.beginWindow(1);

    //Expire
    expire.setValue(true);

    ExceptionSaverExceptionHandler eseh = new ExceptionSaverExceptionHandler();
    testBlockingNoStop(sdqqm, eseh);

    query = new MockQuery("2");
    sdqqm.enqueue(query, null, new MutableBoolean(false));

    Thread.sleep(1000);

    Assert.assertNull(eseh.getCaughtThrowable());

    sdqqm.endWindow();
    sdqqm.teardown();
  }

  @SuppressWarnings({"deprecation", "CallToThreadStopSuspendOrResumeManager"})
  private void testBlocking(SimpleDoneQueueManager<Query, Void> sdqqm) throws InterruptedException
  {
    Thread thread = new Thread(new BlockedThread<Query, Void, MutableBoolean>(sdqqm));
    //thread.setUncaughtExceptionHandler(new RethrowExceptionHandler(Thread.currentThread()));
    thread.start();
    Thread.sleep(100);

    Assert.assertEquals(Thread.State.WAITING, thread.getState());

    thread.stop();
  }

  private Thread testBlockingNoStop(SimpleDoneQueueManager<Query, Void> sdqqm,
                                    ExceptionSaverExceptionHandler eseh) throws InterruptedException
  {
    Thread thread = new Thread(new BlockedThread<Query, Void, MutableBoolean>(sdqqm));
    thread.setUncaughtExceptionHandler(eseh);
    thread.start();
    Thread.sleep(100);

    Assert.assertEquals(Thread.State.WAITING, thread.getState());

    return thread;
  }

  public static class BlockedThread<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> implements Runnable
  {
    QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queueManager;

    public BlockedThread(QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queueManager)
    {
      setQueueManager(queueManager);
    }

    private void setQueueManager(QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queueManager)
    {
      this.queueManager = Preconditions.checkNotNull(queueManager);
    }

    @Override
    public void run()
    {
      LOG.debug("{}", queueManager.dequeueBlock());
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(SimpleDoneQueryQueueManagerTest.class);
}
