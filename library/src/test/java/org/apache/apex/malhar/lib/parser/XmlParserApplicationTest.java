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
package org.apache.apex.malhar.lib.parser;

import java.util.concurrent.Callable;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.io.ConsoleOutputOperator;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.StramLocalCluster;

/**
 * Test to check if a tuple class can be set properly for XMLParser and
 * a simple sample app is running fine for the same.(Ref. APEXMALHAR-2316 and APEXMALHAR-2346)
 */
public class XmlParserApplicationTest
{
  public static int TupleCount;
  public static org.apache.apex.malhar.lib.parser.XmlParserTest.EmployeeBean obj;

  @Test
  public void testApplication()
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      DAG dag = lma.getDAG();
      XmlDataEmitterOperator input = dag.addOperator("data", new XmlDataEmitterOperator());
      XmlParser parser = dag.addOperator("xmlparser", new XmlParser());
      ResultCollector rc = dag.addOperator("rc", new ResultCollector());
      dag.getMeta(parser).getMeta(parser.out).getAttributes().put(Context.PortContext.TUPLE_CLASS, org.apache.apex.malhar.lib.parser.XmlParserTest.EmployeeBean.class);
      ConsoleOutputOperator xmlObjectOp = dag.addOperator("xmlObjectOp", new ConsoleOutputOperator());
      xmlObjectOp.setDebug(true);
      dag.addStream("input", input.output, parser.in);
      dag.addStream("output", parser.parsedOutput, xmlObjectOp.input);
      dag.addStream("pojo", parser.out,rc.input);
      LocalMode.Controller lc = lma.getController();
      lc.setHeartbeatMonitoringEnabled(false);
      ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
      {
        @Override
        public Boolean call() throws Exception
        {
          return TupleCount == 1;
        }
      });
      lc.run(10000);// runs for 10 seconds and quits
      Assert.assertEquals(1,TupleCount);
      Assert.assertEquals("john", obj.getName());
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  public static class XmlDataEmitterOperator extends BaseOperator implements InputOperator
  {
    public static String xmlSample = "<com.datatorrent.contrib.schema.parser.XmlParserTest_-EmployeeBean>" + "<name>john</name>"
        + "<dept>cs</dept>" + "<eid>1</eid>" + "<dateOfJoining>2015-01-01</dateOfJoining>" + "<address>"
        + "<city>new york</city>" + "<country>US</country>" + "</address>"
        + "</com.datatorrent.contrib.schema.parser.XmlParserTest_-EmployeeBean>";
    public static boolean emitTuple = true;

    public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

    @Override
    public void emitTuples()
    {
      if (emitTuple ) {
        output.emit(xmlSample);
        emitTuple = false;
      }
    }
  }

  public static class ResultCollector extends BaseOperator
  {
    public final transient DefaultInputPort<java.lang.Object> input = new DefaultInputPort<java.lang.Object>()
    {
      @Override
      public void process(java.lang.Object in)
      {
        obj = (XmlParserTest.EmployeeBean)in;
        TupleCount++;
      }
    };

    @Override
    public void setup(Context.OperatorContext context)
    {
      TupleCount = 0;
    }
  }

}
