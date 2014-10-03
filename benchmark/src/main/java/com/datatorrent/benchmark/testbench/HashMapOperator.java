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

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * HashMap Input Operator used as a helper in testbench benchmarking apps.
 */
public class HashMapOperator implements InputOperator
{
  public final transient DefaultOutputPort<HashMap<String, Double>> hmap_data = new DefaultOutputPort<HashMap<String, Double>>();
  public final transient DefaultOutputPort<HashMap<String, ArrayList<Integer>>> hmapList_data = new DefaultOutputPort<HashMap<String, ArrayList<Integer>>>();
  public final transient DefaultOutputPort<HashMap<String, HashMap<String, Integer>>> hmapMap_data = new DefaultOutputPort<HashMap<String, HashMap<String, Integer>>>();
  public final transient DefaultOutputPort<HashMap<String, Integer>> hmapInt_data = new DefaultOutputPort<HashMap<String, Integer>>();

  @Override
  public void emitTuples()
  {
    HashMap<String, Double> hmap = new HashMap<String, Double>();
    HashMap<String, ArrayList<Integer>> hmapList = new HashMap<String, ArrayList<Integer>>();
    HashMap<String, HashMap<String, Integer>> hmapMap = new HashMap<String, HashMap<String, Integer>>();
    ArrayList<Integer> list = new ArrayList<Integer>();
    HashMap<String, Integer> hmapMapTemp;
    int numTuples = 1000;
    Integer aval = 1000;
    Integer bval = 100;

    HashMap<String, ArrayList<Integer>> stuple = new HashMap<String, ArrayList<Integer>>();
    //int numtuples = 100000000; // For benchmarking
    int numtuples = 1000;
    String seed1 = "a";
    ArrayList val = new ArrayList();
    val.add(10);
    val.add(20);
    stuple.put(seed1, val);
     if (hmapList_data.isConnected()) {
        hmapList_data.emit(stuple);
      }
    for (int i = 0; i < numTuples; i++) {
      hmap.clear();
      //list.clear();
      //hmapList.clear();
      hmapMapTemp = new HashMap<String, Integer>();
      hmapMap.clear();
      hmap.put("ia", 2.0);
      hmap.put("ib", 20.0);
      hmap.put("ic", 1000.0);
      hmap.put("id", 1000.0);
      if (hmap_data.isConnected()) {
        hmap_data.emit(hmap);
      }

      //list.add(i);
      //hmapList.put("x", list);
      //list.add(20);
      //hmapList.put("y", list);



      hmapMapTemp.put("a", aval);
      hmapMapTemp.put("b", bval);

      if (hmapInt_data.isConnected()) {
        hmapInt_data.emit(hmapMapTemp);
      }

      hmapMap.put("x", hmapMapTemp);
      hmapMap.put("y", hmapMapTemp);

      if (hmapMap_data.isConnected()) {
        hmapMap_data.emit(hmapMap);
      }
    }

  }

  @Override
  public void beginWindow(long windowId)
  {
    // throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void endWindow()
  {

  }

  @Override
  public void setup(OperatorContext context)
  {
    // throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void teardown()
  {
    // throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

}
