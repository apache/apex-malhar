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
package com.datatorrent.benchmark;

/*
 * To change this template, choose Tools | Templates and open the template in the editor.
 */
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Context.OperatorContext;

import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>WordCountOperator class.</p>
 *
 * @since 0.3.2
 */
public class WordCountOperator<T> implements Operator
{
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      count++;
    }

  };
  private transient ArrayList<Integer> counts;
  private transient int count;
  private long startmillis;
  private ArrayList<Integer> millis;

  @Override
  public void endWindow()
  {
    counts.add(count);
    millis.add((int)(System.currentTimeMillis() - startmillis));
    count = 0;

    if (counts.size() % 10 == 0) {
      logger.info("millis = {}", millis);
      logger.info("counts = {}", counts);
      millis.clear();
      counts.clear();
    }
  }

  @Override
  public void teardown()
  {
    logger.info("millis = {}", millis);
    logger.info("counts = {}", counts);
  }

  @Override
  public void beginWindow(long windowId)
  {
    startmillis = System.currentTimeMillis();
  }

  @Override
  public void setup(OperatorContext context)
  {
    counts = new ArrayList<Integer>();
    millis = new ArrayList<Integer>();
  }
  private static final Logger logger = LoggerFactory.getLogger(WordCountOperator.class);
}
