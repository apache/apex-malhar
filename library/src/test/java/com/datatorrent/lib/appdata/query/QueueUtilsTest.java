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

import com.datatorrent.lib.appdata.QueueUtils.ConditionBarrier;

public class QueueUtilsTest
{
  @Test
  public void conditionBarrierBlockTest() throws Exception
  {
    ConditionBarrier cb = new ConditionBarrier();
    cb.lock();

    blockBarrierTest(cb);
  }

  @Test
  public void conditionBarrierPassTest() throws Exception
  {
    ConditionBarrier cb = new ConditionBarrier();

    passBarrierTest(cb);
  }

  @Test
  public void passThenBlockThenPassTest() throws Exception
  {
    ConditionBarrier cb = new ConditionBarrier();

    passBarrierTest(cb);
    cb.lock();
    blockBarrierTest(cb);
    cb.unlock();
    passBarrierTest(cb);
  }

  private void passBarrierTest(ConditionBarrier cb) throws Exception
  {
    Thread thread = new Thread(new BarrierThread(cb));
    thread.start();
    Thread.sleep(100);

    Assert.assertEquals(Thread.State.TERMINATED, thread.getState());
  }

  private void blockBarrierTest(ConditionBarrier cb) throws Exception
  {
    Thread thread = new Thread(new BarrierThread(cb));
    thread.start();
    Thread.sleep(100);

    Assert.assertEquals(Thread.State.WAITING, thread.getState());
  }

  public static class BarrierThread implements Runnable
  {
    private ConditionBarrier cb;

    public BarrierThread(ConditionBarrier cb)
    {
      this.cb = Preconditions.checkNotNull(cb);
    }

    @Override
    public void run()
    {
      cb.gate();
    }
  }
}
