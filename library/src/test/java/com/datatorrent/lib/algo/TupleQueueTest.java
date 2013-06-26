/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.algo;

import com.datatorrent.api.Sink;
import com.datatorrent.lib.algo.TupleQueue;
import com.datatorrent.tuple.Tuple;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.algo.TupleQueue} <p>
 *
 */
public class TupleQueueTest
{
  private static Logger log = LoggerFactory.getLogger(TupleQueueTest.class);

  class QueueSink implements Sink
  {
    int count = 0;
    @Override
    public void put(Object payload)
    {
      if (payload instanceof Tuple) {
      }
      else {
        HashMap<String, Object> tuple = (HashMap<String, Object>)payload;
        for (Map.Entry<String, Object> e: tuple.entrySet()) {
          count++;
        }
      }
    }

    @Override
    public int getCount(boolean reset)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  class ConsoleSink implements Sink
  {
    int count = 0;
    HashMap<String, ArrayList> map  = new HashMap<String, ArrayList>();
    @Override
    public void put(Object payload)
    {
      if (payload instanceof Tuple) {
      }
      else {
        HashMap<String, ArrayList> tuple = (HashMap<String, ArrayList>)payload;
        for (Map.Entry<String, ArrayList> e: tuple.entrySet()) {
          map.put(e.getKey(), e.getValue());
          count++;
        }
      }
    }

    public String print() {
      String line = "\nTotal of ";
      line += String.format("%d tuples\n", count);
      for (Map.Entry<String, ArrayList> e: map.entrySet()) {
        line += e.getKey() +": ";
        for (Object o : e.getValue()) {
          line += ",";
          line += o.toString();
        }
        line += "\n";
      }
      return line;
    }

    @Override
    public int getCount(boolean reset)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }


  /**
   * Test oper logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    final TupleQueue oper = new TupleQueue();

    QueueSink queueSink = new QueueSink();
    ConsoleSink consoleSink = new ConsoleSink();

    Sink dataSink = oper.data.getSink();
    Sink querySink = oper.query.getSink();
    oper.queue.setSink(queueSink);
    oper.console.setSink(consoleSink);

    oper.beginWindow(0);

    HashMap<String, Integer> dinput = null;
    int numtuples = 100;
    for (int i = 0; i < numtuples; i++) {
      dinput = new HashMap<String, Integer>();
      dinput.put("a", new Integer(i));
      dinput.put("b", new Integer(100+i));
      dinput.put("c", new Integer(200+i));
      dataSink.put(dinput);
    }

    oper.endWindow();

    oper.beginWindow(0);
    String key = "a";
    querySink.put(key);
    key = "b";
    querySink.put(key);
    key = "c";
    querySink.put(key);


    oper.endWindow();

    log.debug(String.format("\n*************************\nQueue had %d tuples\n", queueSink.count));
    log.debug(consoleSink.print());
  }
}
