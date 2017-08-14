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
package org.apache.apex.benchmark.testbench;

import java.util.ArrayList;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.testbench.EventGenerator;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 * HashMap Input Operator used as a helper in testbench benchmarking apps.
 *
 * @since 2.0.0
 */
public class HashMapOperator implements InputOperator
{
  private String keys = null;
  private static final Logger logger = LoggerFactory.getLogger(EventGenerator.class);
  private String[] keysArray = {"a", "b", "c", "d"};
  public final transient DefaultOutputPort<HashMap<String, Double>> hmap_data =
      new DefaultOutputPort<HashMap<String, Double>>();
  public final transient DefaultOutputPort<HashMap<String, ArrayList<Integer>>> hmapList_data =
      new DefaultOutputPort<HashMap<String, ArrayList<Integer>>>();
  public final transient DefaultOutputPort<HashMap<String, HashMap<String, Integer>>> hmapMap_data =
      new DefaultOutputPort<HashMap<String, HashMap<String, Integer>>>();
  public final transient DefaultOutputPort<HashMap<String, Integer>> hmapInt_data =
      new DefaultOutputPort<HashMap<String, Integer>>();
  private int numTuples = 1000;
  private String seed = "a";
  private int numKeys = 2;

  public String getSeed()
  {
    return seed;
  }

  public void setSeed(String seed)
  {
    this.seed = seed;
  }

  public String getKeys()
  {
    return keys;
  }

  public void setKeys(String keys)
  {
    logger.debug("in hash map key setter");
    this.keys = keys;
    keysArray = keys.split(",");
  }

  @Override
  public void emitTuples()
  {
    if (hmap_data.isConnected()) {
      HashMap<String, Double> hmap = new HashMap<String, Double>();
      for (int i = 0; i < numTuples; i++) {
        hmap.clear();
        for (int j = 0; j < numKeys; j++) {
          hmap.put(keysArray[j], 2.0 + j * 20);
        }
        hmap_data.emit(hmap);
      }
    }

    if (hmapMap_data.isConnected()) {
      HashMap<String, HashMap<String, Integer>> hmapMap = new HashMap<String, HashMap<String, Integer>>();
      for (int i = 0; i < numTuples; i++) {
        hmapMap.clear();
        HashMap<String, Integer> hmapMapTemp = new HashMap<String, Integer>();
        for (int j = 0; j < numKeys; j++) {
          hmapMapTemp.put(keysArray[j], 100 * j);
        }
        for (int j = 0; j < numKeys; j++) {
          hmapMap.put(keysArray[j], hmapMapTemp);
        }
        hmapMap_data.emit(hmapMap);
      }
    }

    if (hmapList_data.isConnected()) {
      HashMap<String, ArrayList<Integer>> stuple = new HashMap<String, ArrayList<Integer>>();
      ArrayList val = new ArrayList();
      for (int i = 1; i < 10; i++) {
        val.add(i);
      }
      stuple.put(seed, val);
      hmapList_data.emit(stuple);
    }

    if (hmapInt_data.isConnected()) {
      for (int i = 0; i < numTuples; i++) {
        HashMap<String, Integer> hmapMapTemp = new HashMap<String, Integer>();
        for (int j = 0; j < numKeys; j++) {
          hmapMapTemp.put(keysArray[j], 100 * j);
        }
        hmapInt_data.emit(hmapMapTemp);
      }
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    // throw new UnsupportedOperationException("Not supported yet.");
    // To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void endWindow()
  {

  }

  @Override
  public void setup(OperatorContext context)
  {
    // throw new UnsupportedOperationException("Not supported yet.");
    // To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void teardown()
  {
    // throw new UnsupportedOperationException("Not supported yet.");
    // To change body of generated methods, choose Tools | Templates.
  }

}
