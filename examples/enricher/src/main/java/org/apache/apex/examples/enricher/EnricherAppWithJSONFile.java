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

package org.apache.apex.examples.enricher;

import java.util.ArrayList;

import org.apache.apex.malhar.contrib.enrich.JsonFSLoader;
import org.apache.apex.malhar.contrib.enrich.POJOEnricher;
import org.apache.apex.malhar.contrib.parser.JsonParser;
import org.apache.apex.malhar.lib.function.Function;
import org.apache.apex.malhar.lib.function.FunctionOperator.MapFunctionOperator;
import org.apache.apex.malhar.lib.function.FunctionOperatorUtil;
import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "EnricherAppWithJSONFile")
/**
 * @since 3.7.0
 */
public class EnricherAppWithJSONFile implements StreamingApplication
{
  @VisibleForTesting
  Function.MapFunction<Object, ?> outputFn = FunctionOperatorUtil.CONSOLE_SINK_FN;
  DataGenerator dataGenerator;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dataGenerator = dag.addOperator("DataGenerator", DataGenerator.class);
    JsonParser parser = dag.addOperator("Parser", JsonParser.class);

    /**
     * FSLoader is used to configure Enricher backend. Property of FSLoader file which is fileName is set in
     * properties.xml file.
     * The format that is used to read the file is present as an example in resources/circleMapping.txt file.
     */
    JsonFSLoader fsLoader = new JsonFSLoader();
    POJOEnricher enrich = dag.addOperator("Enrich", POJOEnricher.class);
    enrich.setStore(fsLoader);

    ArrayList<String> includeFields = new ArrayList<>();
    includeFields.add("circleName");
    ArrayList<String> lookupFields = new ArrayList<>();
    lookupFields.add("circleId");

    enrich.setIncludeFields(includeFields);
    enrich.setLookupFields(lookupFields);

    MapFunctionOperator<Object, ?> out = dag.addOperator("out", new MapFunctionOperator<>(outputFn));
    dag.addStream("Parse", dataGenerator.output, parser.in);
    dag.addStream("Enrich", parser.out, enrich.input);
    dag.addStream("Console", enrich.output, out.input);
  }

  public DataGenerator getDataGenerator()
  {
    return dataGenerator;
  }

  public void setDataGenerator(DataGenerator dataGenerator)
  {
    this.dataGenerator = dataGenerator;
  }
}
