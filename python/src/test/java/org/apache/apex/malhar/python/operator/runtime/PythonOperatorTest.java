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
package org.apache.apex.malhar.python.operator.runtime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.PythonConstants;
import org.apache.apex.malhar.python.operator.transform.PythonFilterOperator;
import org.apache.apex.malhar.python.operator.transform.PythonFlatMapOperator;
import org.apache.apex.malhar.python.operator.transform.PythonMapOperator;
import org.apache.apex.malhar.python.runtime.PythonWorkerContext;
import org.apache.commons.codec.binary.Base64;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.StramLocalCluster;

public class PythonOperatorTest
{
  private static int TupleCount;
  private static List<Integer> lengthList = new ArrayList<>();
  private static int tupleSumInResultCollector = 0;

  private static final int SourcedNumTuples = 10;
  private static int ExpectedNumTuples = 0;
  private static final Logger LOG = LoggerFactory.getLogger(PythonOperatorTest.class);

  public static class NumberGenerator extends BaseOperator implements InputOperator
  {
    private int num;

    public final transient DefaultOutputPort<Integer> output = new DefaultOutputPort<Integer>();

    @Override
    public void setup(Context.OperatorContext context)
    {
      num = 0;
    }

    @Override
    public void emitTuples()
    {

      if (num < SourcedNumTuples) {
        output.emit(new Integer(num));
        num++;
      }
    }
  }

  public static class ResultCollector extends BaseOperator
  {

    public final transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>()
    {

      @Override
      public void process(Integer in)
      {

        LOG.debug("Input data " + in);
        TupleCount++;
        lengthList.add(in);
        tupleSumInResultCollector += in;
      }
    };

    @Override
    public void setup(Context.OperatorContext context)
    {
      TupleCount = 0;
      tupleSumInResultCollector = 0;

      lengthList = new ArrayList<>();
    }

  }

  @Test
  public void testPythonMapOperator()
  {

    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    ExpectedNumTuples = SourcedNumTuples;

    NumberGenerator numGen = dag.addOperator("numGen", new NumberGenerator());

    ResultCollector collector = dag.addOperator("collector", new ResultCollector());
//    Base64 string for map function written and serialized using pickle
//    Function :
//         lambda(a): int(a)*2
    String funcAsBase64String = "gAJjY2xvdWRwaWNrbGUuY2xvdWRwaWNrbGUKX2ZpbGxfZnVuY3Rpb24KcQAoY2Nsb3VkcGlja2xlLmNsb3VkcGlja2xlCl9tYWtlX3NrZWxfZnVuYwpxAWNjbG91ZHBpY2tsZS5jbG91ZHBpY2tsZQpfYnVpbHRpbl90eXBlCnECVQhDb2RlVHlwZXEDhXEEUnEFKEsBSwFLAktDVQ50AAB8AACDAQBkAQAUU3EGTksChnEHVQNpbnRxCIVxCVUBZnEKhXELVR48aXB5dGhvbi1pbnB1dC00LTU4NDRlZWUwNzQ4Zj5xDFUIPGxhbWJkYT5xDUsBVQBxDikpdHEPUnEQXXERfXESh3ETUnEUfXEVTn1xFnRSLg==";

    byte[] decoded = Base64.decodeBase64(funcAsBase64String);

    Map<String, String> environmentData = new HashMap<>();

    final String cwd = System.getProperty("user.dir");
    String pythonRuntimeDirectory = cwd + "/../python/apex-python/src/pyapex/runtime";
    String pythonDepsDirectory = cwd + "/../python/apex-python/deps";

    LOG.debug("Current working directory:" + pythonRuntimeDirectory);
    environmentData.put(PythonWorkerContext.PYTHON_WORKER_PATH, pythonRuntimeDirectory + "/" + PythonConstants.PYTHON_WORKER_FILE_NAME);
    environmentData.put(PythonWorkerContext.PY4J_DEPENDENCY_PATH, pythonDepsDirectory + "/" + PythonConstants.PY4J_SRC_ZIP_FILE_NAME);
    environmentData.put(PythonWorkerContext.PYTHON_APEX_PATH, pythonDepsDirectory + "/" + PythonConstants.PYTHON_APEX_ZIP_NAME);
    PythonMapOperator<Integer> mapOperator = new PythonMapOperator<Integer>(decoded);
    mapOperator.getServer().setPythonOperatorEnv(environmentData);

    dag.addOperator("mapOperator", mapOperator);

    dag.addStream("raw numbers", numGen.output, mapOperator.in);
    dag.addStream("mapped results", mapOperator.out, collector.input);

    // Create local cluster
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {

        return TupleCount == ExpectedNumTuples;
      }
    });

    lc.run(100000);

    Assert.assertEquals(ExpectedNumTuples, TupleCount);
    Assert.assertEquals(90, tupleSumInResultCollector);
  }

  @Test
  public void testFilterOperator()
  {

    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    NumberGenerator numGen = dag.addOperator("numGen", new NumberGenerator());

    ResultCollector collector = dag.addOperator("collector", new ResultCollector());
    ExpectedNumTuples = SourcedNumTuples - 2;
//    def f(a):
//        if a< 2:
//             return False
//        return True

    String funcAsBase64String = "gAJjZGlsbC5kaWxsCl9jcmVhdGVfZnVuY3Rpb24KcQAoY2RpbGwuZGlsbApfbG9hZF90eXBlCnEBVQhDb2RlVHlwZXEChXEDUnEEKEsBSwFLAktDVRR8AABkAQBrAAByEAB0AABTdAEAU3EFTksChnEGVQVGYWxzZXEHVQRUcnVlcQiGcQlVAWFxCoVxC1UePGlweXRob24taW5wdXQtMS05NDcxMmNkN2IyY2I+cQxVAWZxDUsBVQYAAQwBBAFxDikpdHEPUnEQY19fYnVpbHRpbl9fCl9fbWFpbl9fCmgNTk59cRF0cRJScRMu";

    byte[] decoded = Base64.decodeBase64(funcAsBase64String);

    Map<String, String> environmentData = new HashMap<>();

    final String cwd = System.getProperty("user.dir");
    String pythonRuntimeDirectory = cwd + "/../python/apex-python/src/pyapex/runtime";
    String pythonDepsDirectory = cwd + "/../python/apex-python/deps";

    LOG.debug("Current working directory:" + pythonRuntimeDirectory);
    environmentData.put(PythonWorkerContext.PYTHON_WORKER_PATH, pythonRuntimeDirectory + "/" + PythonConstants.PYTHON_WORKER_FILE_NAME);
    environmentData.put(PythonWorkerContext.PY4J_DEPENDENCY_PATH, pythonDepsDirectory + "/" + PythonConstants.PY4J_SRC_ZIP_FILE_NAME);
    environmentData.put(PythonWorkerContext.PYTHON_APEX_PATH, pythonDepsDirectory + "/" + PythonConstants.PYTHON_APEX_ZIP_NAME);
    PythonFilterOperator<Integer> mapOperator = new PythonFilterOperator<Integer>(decoded);
    mapOperator.getServer().setPythonOperatorEnv(environmentData);

    dag.addOperator("mapOperator", mapOperator);

    dag.addStream("raw numbers", numGen.output, mapOperator.in);
    dag.addStream("mapped results", mapOperator.out, collector.input);

    // Create local cluster
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        return TupleCount == ExpectedNumTuples;
      }
    });

    lc.run(100000);

    Assert.assertEquals(ExpectedNumTuples, TupleCount);
    Assert.assertEquals(44, tupleSumInResultCollector);
  }

  @Test
  public void testFlatMapOperator()
  {

    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    ExpectedNumTuples = SourcedNumTuples * 2;
    NumberGenerator numGen = dag.addOperator("numGen", new NumberGenerator());

    ResultCollector collector = dag.addOperator("collector", new ResultCollector());
//    def f(a):
//        return [int(a)*2, int(a)*3]

    String funcAsBase64String = "gAJjZGlsbC5kaWxsCl9jcmVhdGVfZnVuY3Rpb24KcQAoY2RpbGwuZGlsbApfbG9hZF90eXBlCnEBVQhDb2RlVHlwZXEChXEDUnEEKEsBSwFLA0tDVR50AAB8AACDAQBkAQAUdAAAfAAAgwEAZAIAFGcCAFNxBU5LAksDh3EGVQNpbnRxB4VxCFUBYXEJhXEKVR48aXB5dGhvbi1pbnB1dC0xLWFjNjk0MzQ3NzhlYT5xC1UBZnEMSwFVAgABcQ0pKXRxDlJxD2NfX2J1aWx0aW5fXwpfX21haW5fXwpoDE5OfXEQdHERUnESLg==";

    byte[] decoded = Base64.decodeBase64(funcAsBase64String);

    Map<String, String> environmentData = new HashMap<>();

    final String cwd = System.getProperty("user.dir");
    String pythonRuntimeDirectory = cwd + "/../python/apex-python/src/pyapex/runtime";
    String pythonDepsDirectory = cwd + "/../python/apex-python/deps";

    LOG.debug("Current working directory:" + pythonRuntimeDirectory);
    environmentData.put(PythonWorkerContext.PYTHON_WORKER_PATH, pythonRuntimeDirectory + "/" + PythonConstants.PYTHON_WORKER_FILE_NAME);
    environmentData.put(PythonWorkerContext.PY4J_DEPENDENCY_PATH, pythonDepsDirectory + "/" + PythonConstants.PY4J_SRC_ZIP_FILE_NAME);
    environmentData.put(PythonWorkerContext.PYTHON_APEX_PATH, pythonDepsDirectory + "/" + PythonConstants.PYTHON_APEX_ZIP_NAME);
    PythonFlatMapOperator<Integer> mapOperator = new PythonFlatMapOperator<Integer>(decoded);
    mapOperator.getServer().setPythonOperatorEnv(environmentData);

    dag.addOperator("mapOperator", mapOperator);

    dag.addStream("raw numbers", numGen.output, mapOperator.in);
    dag.addStream("mapped results", mapOperator.out, collector.input);

    // Create local cluster
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        return TupleCount == ExpectedNumTuples;
      }
    });

    lc.run(100000);

    Assert.assertEquals(ExpectedNumTuples, TupleCount);
    Assert.assertEquals(225, tupleSumInResultCollector);
  }

}
