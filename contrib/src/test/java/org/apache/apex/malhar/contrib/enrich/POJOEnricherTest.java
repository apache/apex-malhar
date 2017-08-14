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
package org.apache.apex.malhar.contrib.enrich;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.TestUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;

public class POJOEnricherTest extends JDBCLoaderTest
{
  @Test
  public void includeSelectedKeys()
  {
    POJOEnricher oper = new POJOEnricher();
    oper.setStore(testMeta.dbloader);
    oper.setLookupFields(Arrays.asList("ID"));
    oper.setIncludeFields(Arrays.asList("NAME", "AGE", "ADDRESS"));
    oper.outputClass = EmployeeOrder.class;
    oper.inputClass = Order.class;
    oper.setup(null);

    CollectorTestSink sink = new CollectorTestSink();
    TestUtils.setSink(oper.output, sink);

    oper.activate(null);

    oper.beginWindow(1);
    Order tuple = new Order(3, 4, 700);
    oper.input.process(tuple);
    oper.endWindow();

    oper.deactivate();

    Assert.assertEquals("includeSelectedKeys: Number of tuples emitted: ", 1, sink.collectedTuples.size());
    Assert.assertEquals("Ouput Tuple: ",
        "{OID=3, ID=4, amount=700.0, NAME='Mark', AGE=25, ADDRESS='Rich-Mond', SALARY=0.0}",
        sink.collectedTuples.get(0).toString());
  }

  @Test
  public void includeAllKeys()
  {
    POJOEnricher oper = new POJOEnricher();
    oper.setStore(testMeta.dbloader);
    oper.setLookupFields(Arrays.asList("ID"));
    oper.setIncludeFields(Arrays.asList("NAME", "AGE", "ADDRESS", "SALARY"));
    oper.outputClass = EmployeeOrder.class;
    oper.inputClass = Order.class;
    oper.setup(null);

    CollectorTestSink sink = new CollectorTestSink();
    TestUtils.setSink(oper.output, sink);

    oper.activate(null);

    oper.beginWindow(1);
    Order tuple = new Order(3, 4, 700);
    oper.input.process(tuple);
    oper.endWindow();

    oper.deactivate();

    Assert.assertEquals("includeSelectedKeys: Number of tuples emitted: ", 1, sink.collectedTuples.size());
    Assert.assertEquals("Ouput Tuple: ",
        "{OID=3, ID=4, amount=700.0, NAME='Mark', AGE=25, ADDRESS='Rich-Mond', SALARY=65000.0}",
        sink.collectedTuples.get(0).toString());
  }

  @Test
  public void testApplication() throws Exception
  {
    EnrichApplication enrichApplication = new EnrichApplication(testMeta);
    enrichApplication.setLoader(testMeta.dbloader);

    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    lma.prepareDAG(enrichApplication, conf);
    LocalMode.Controller lc = lma.getController();
    lc.run(10000);// runs for 10 seconds and quits
  }

  public static class EnrichApplication implements StreamingApplication
  {
    private final TestMeta testMeta;
    BackendLoader loader;

    public EnrichApplication(TestMeta testMeta)
    {
      this.testMeta = testMeta;
    }

    @Override
    public void populateDAG(DAG dag, Configuration configuration)
    {
      RandomPOJOGenerator input = dag.addOperator("Input", RandomPOJOGenerator.class);
      POJOEnricher enrich = dag.addOperator("Enrich", POJOEnricher.class);
      EnrichVerifier verify = dag.addOperator("Verify", EnrichVerifier.class);
      verify.address = testMeta.address;
      verify.age = testMeta.age;
      verify.names = testMeta.name;
      verify.salary = testMeta.salary;

      enrich.setStore(loader);
      ArrayList<String> lookupFields = new ArrayList<>();
      lookupFields.add("ID");
      ArrayList<String> includeFields = new ArrayList<>();
      includeFields.add("NAME");
      includeFields.add("AGE");
      includeFields.add("ADDRESS");
      includeFields.add("SALARY");
      enrich.setLookupFields(lookupFields);
      enrich.setIncludeFields(includeFields);

      dag.getMeta(enrich).getMeta(enrich.input).getAttributes().put(Context.PortContext.TUPLE_CLASS, Order.class);
      dag.getMeta(enrich).getMeta(enrich.output).getAttributes()
          .put(Context.PortContext.TUPLE_CLASS, EmployeeOrder.class);

      dag.addStream("S1", input.output, enrich.input);
      dag.addStream("S2", enrich.output, verify.input);
    }

    public void setLoader(BackendLoader loader)
    {
      this.loader = loader;
    }
  }

  public static class RandomPOJOGenerator implements InputOperator
  {
    public transient DefaultOutputPort<Object> output = new DefaultOutputPort<>();
    private int idx = 0;
    private boolean emit = true;

    @Override
    public void emitTuples()
    {
      if (!emit) {
        return;
      }
      idx += idx++ % 4;
      Order o = new Order(1, idx + 1, 100.00);
      output.emit(o);
      if (idx == 3) {
        emit = false;
      }
    }

    @Override
    public void beginWindow(long l)
    {
      emit = true;
    }

    @Override
    public void endWindow()
    {

    }

    @Override
    public void setup(Context.OperatorContext context)
    {
    }

    @Override
    public void teardown()
    {
    }
  }

  public static class EnrichVerifier extends BaseOperator
  {
    private static final Logger logger = LoggerFactory.getLogger(EnrichVerifier.class);
    String[] names;
    int[] age;
    String[] address;
    double[] salary;

    private transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object o)
      {
        Assert.assertTrue(o instanceof EmployeeOrder);
        EmployeeOrder order = (EmployeeOrder)o;
        int id = order.getID();
        Assert.assertTrue(id >= 1 && id <= 4);
        Assert.assertEquals(1, order.getOID());
        Assert.assertEquals(100.00, order.getAmount(), 0);

        Assert.assertEquals(names[id - 1], order.getNAME());
        Assert.assertEquals(age[id - 1], order.getAGE());
        Assert.assertEquals(address[id - 1], order.getADDRESS());
        Assert.assertEquals(salary[id - 1], order.getSALARY(), 0);
      }
    };
  }
}

