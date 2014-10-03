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
package com.datatorrent.benchmark.testbench;

import com.datatorrent.api.LocalMode;
import com.datatorrent.lib.testbench.EventClassifier;
import java.util.ArrayList;
import java.util.HashMap;
import org.junit.Test;

/**
 * Benchmark Test for EventClassifier Operator in local mode.
 */
public class EventClassifierAppTest
{
  @Test
  public void testEventClassifierApp() throws Exception
  {
    HashMap<String, Double> keymap = new HashMap<String, Double>();
    keymap.put("a", 1.0);
    keymap.put("b", 4.0);
    keymap.put("c", 5.0);

    EventClassifier eventInput = new EventClassifier();
    eventInput.setKeyMap(keymap);
    eventInput.setOperationReplace();

    HashMap<String, ArrayList<Integer>> wmap = new HashMap<String, ArrayList<Integer>>();
    ArrayList<Integer> list = new ArrayList<Integer>(3);
    list.add(60);
    list.add(10);
    list.add(35);
    wmap.put("ia", list);
    list = new ArrayList<Integer>(3);
    list.add(10);
    list.add(75);
    list.add(15);
    wmap.put("ib", list);
    list = new ArrayList<Integer>(3);
    list.add(20);
    list.add(10);
    list.add(70);
    wmap.put("ic", list);
    list = new ArrayList<Integer>(3);
    list.add(50);
    list.add(15);
    list.add(35);
    wmap.put("id", list);
    eventInput.setKeyWeights(wmap);

    LocalMode.runApp(new EventClassifierApp(), 30000);
  }

}
