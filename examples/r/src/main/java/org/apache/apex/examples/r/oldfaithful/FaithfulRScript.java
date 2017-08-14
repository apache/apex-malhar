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
package org.apache.apex.examples.r.oldfaithful;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.contrib.r.RScript;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

/**
 * @since 2.1.0
 */
public class FaithfulRScript extends RScript
{

  private transient List<FaithfulKey> readingsList = new ArrayList<FaithfulKey>();
  private int elapsedTime;
  private static final Logger LOG = LoggerFactory.getLogger(FaithfulRScript.class);

  public FaithfulRScript()
  {
    super();
  }

  public FaithfulRScript(String rScriptFilePath, String rFunction, String returnVariable)
  {
    super(rScriptFilePath, rFunction, returnVariable);
  }

  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<FaithfulKey> faithfulInput = new DefaultInputPort<FaithfulKey>()
  {
    @Override
    public void process(FaithfulKey tuple)
    {
      // Create a map of ("String", values) to be passed to the process
      // function in the RScipt operator's process()
      readingsList.add(tuple);

    }

  };

  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<Integer> inputElapsedTime = new DefaultInputPort<Integer>()
  {
    @Override
    public void process(Integer eT)
    {
      elapsedTime = eT;
    }
  };

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
  }

  @Override
  public void endWindow()
  {
    if (readingsList.size() == 0) {
      return;
    }
    LOG.info("Input data size: readingsList - " + readingsList.size());

    double[] eruptionDuration = new double[readingsList.size()];
    int[] waitingTime = new int[readingsList.size()];

    for (int i = 0; i < readingsList.size(); i++) {
      eruptionDuration[i] = readingsList.get(i).getEruptionDuration();
      waitingTime[i] = readingsList.get(i).getWaitingTime();
    }
    LOG.info("Input data size: eruptionDuration - " + eruptionDuration.length);
    LOG.info("Input data size: waitingTime - " + waitingTime.length);

    HashMap<String, Object> map = new HashMap<String, Object>();

    map.put("ELAPSEDTIME", elapsedTime);
    map.put("ERUPTIONS", eruptionDuration);
    map.put("WAITING", waitingTime);

    super.process(map);
    readingsList.clear();
    map.clear();
  }
}
