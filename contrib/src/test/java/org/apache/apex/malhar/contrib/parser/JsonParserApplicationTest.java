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
package org.apache.apex.malhar.contrib.parser;

import java.io.IOException;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.contrib.parser.JsonParserTest.Product;
import org.apache.apex.malhar.lib.appdata.schemas.SchemaUtils;
import org.apache.apex.malhar.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;

public class JsonParserApplicationTest
{

  @Test
  public void testApplication() throws IOException, Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      lma.prepareDAG(new JsonParserTest(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(10000);// runs for 10 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  public static class JsonParserTest implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      JsonDataEmitterOperator input = dag.addOperator("data", new JsonDataEmitterOperator());
      JsonParser parser = dag.addOperator("jsonparser", new JsonParser());
      parser.setClazz(Product.class);
      dag.getMeta(parser).getMeta(parser.out).getAttributes().put(Context.PortContext.TUPLE_CLASS, Product.class);
      parser.setJsonSchema(SchemaUtils.jarResourceFileToString("json-parser-schema.json"));
      ConsoleOutputOperator jsonObjectOp = dag.addOperator("jsonObjectOp", new ConsoleOutputOperator());
      ConsoleOutputOperator pojoOp = dag.addOperator("pojoOp", new ConsoleOutputOperator());
      jsonObjectOp.setDebug(true);
      dag.addStream("input", input.output, parser.in);
      dag.addStream("output", parser.parsedOutput, jsonObjectOp.input);
      dag.addStream("pojo", parser.out, pojoOp.input);
    }
  }

  public static class JsonDataEmitterOperator extends BaseOperator implements InputOperator
  {
    public static String jsonSample = "{" + "\"id\": 2," + "\"name\": \"An ice sculpture\"," + "\"price\": 1,"
        + "\"tags\": [\"cold\", \"ice\"]," + "\"dimensions\": {" + "\"length\": 7.0," + "\"width\" : 8.0,"
        + "\"height\": 9.5" + "}," + "\"warehouseLocation\": {" + "\"latitude\": -78.75," + "\"longitude\": 20.4"
        + "}," + "\"dateOfManufacture\": \"2013/09/29\"," + "\"dateOfExpiry\": \"2013\"" + "}";

    public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<byte[]>();

    @Override
    public void emitTuples()
    {
      output.emit(jsonSample.getBytes());
    }
  }

}
