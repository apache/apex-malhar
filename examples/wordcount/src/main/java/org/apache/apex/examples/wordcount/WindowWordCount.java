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
package org.apache.apex.examples.wordcount;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Computes word frequencies per window and emits them at each {@code endWindow()}. The output is a
 * list of (word, frequency) pairs
 *
 * @since 3.2.0
 */
public class WindowWordCount extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(WindowWordCount.class);

  /** {@literal (word => frequency)} map for current window */
  protected Map<String, WCPair> wordMap = new HashMap<>();

  /**
   * Input port on which words are received
   */
  public final transient DefaultInputPort<String> input = new DefaultInputPort<String>()
  {
    @Override
    public void process(String word)
    {
      WCPair pair = wordMap.get(word);
      if (null != pair) {    // word seen previously
        pair.freq += 1;
        return;
      }

      // new word
      pair = new WCPair();
      pair.word = word;
      pair.freq = 1;
      wordMap.put(word, pair);
    }
  };

  /**
   * Output port which emits the list of word frequencies for current window
   */
  public final transient DefaultOutputPort<List<WCPair>> output = new DefaultOutputPort<>();

  /**
   * {@inheritDoc}
   * If we've seen some words in this window, emit the map and clear it for next window
   */
  @Override
  public void endWindow()
  {
    LOG.info("WindowWordCount: endWindow");

    // got EOF; if no words found, do nothing
    if (wordMap.isEmpty()) {
      return;
    }

    // have some words; emit single map and reset for next file
    final ArrayList<WCPair> list = new ArrayList<>(wordMap.values());
    output.emit(list);
    list.clear();
    wordMap.clear();
  }

}
