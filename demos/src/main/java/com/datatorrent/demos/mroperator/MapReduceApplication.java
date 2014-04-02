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
package com.datatorrent.demos.mroperator;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAGContext;
import com.datatorrent.api.StreamingApplication;

/**
 * <p>
 * Abstract MapReduceApplication class.
 * </p>
 *
 * @since 0.9.0
 */
@SuppressWarnings({ "deprecation" })
@ApplicationAnnotation(name="MapReduceApplication")
public abstract class MapReduceApplication<K1, V1, K2, V2> implements StreamingApplication
{

  private static final Logger logger = LoggerFactory.getLogger(MapReduceApplication.class);

  Class<? extends InputFormat<K1, V1>> inputFormat;
  Class<? extends Mapper<K1, V1, K2, V2>> mapClass;
  Class<? extends Reducer<K2, V2, K2, V2>> reduceClass;
  Class<? extends Reducer<K2, V2, K2, V2>> combineClass;

  public Class<? extends Reducer<K2, V2, K2, V2>> getCombineClass()
  {
    return combineClass;
  }

  public void setCombineClass(Class<? extends Reducer<K2, V2, K2, V2>> combineClass)
  {
    this.combineClass = combineClass;
  }

  public void setInputFormat(Class<? extends InputFormat<K1, V1>> inputFormat)
  {
    this.inputFormat = inputFormat;
  }

  public Class<? extends Mapper<K1, V1, K2, V2>> getMapClass()
  {
    return mapClass;
  }

  public void setMapClass(Class<? extends Mapper<K1, V1, K2, V2>> mapClass)
  {
    this.mapClass = mapClass;
  }

  public Class<? extends Reducer<K2, V2, K2, V2>> getReduceClass()
  {
    return reduceClass;
  }

  public void setReduceClass(Class<? extends Reducer<K2, V2, K2, V2>> reduceClass)
  {
    this.reduceClass = reduceClass;
  }

  public abstract void conf();

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    conf();

    String dirName = conf.get(this.getClass().getName() + ".inputDirName", "src/test/resources/wordcount/");
    String outputDirName = conf.get(this.getClass().getName() + ".outputDirName", "src/test/resources/output");
    int numberOfReducers = conf.getInt(this.getClass().getName() + ".numOfReducers", 1);
    int numberOfMaps = conf.getInt(this.getClass().getName() + ".numOfMaps", 2);
    String configurationfilePath = conf.get(this.getClass().getName() + ".configFile", "");

    // logger.info("configfile {}", configurationfilePath);
    MapOperator<K1, V1, K2, V2> inputOperator = dag.addOperator("map", new MapOperator<K1, V1, K2, V2>());
    inputOperator.setInputFormatClass(inputFormat);
    inputOperator.setDirName(dirName);
    dag.setAttribute(inputOperator, OperatorContext.INITIAL_PARTITION_COUNT, numberOfMaps);

    String configFileName = null;
    if (configurationfilePath != null && !configurationfilePath.isEmpty()) {
      dag.setAttribute(DAGContext.LIBRARY_JARS, configurationfilePath);
      StringTokenizer configFileTokenizer = new StringTokenizer(configurationfilePath, "/");
      configFileName = configFileTokenizer.nextToken();
      while (configFileTokenizer.hasMoreTokens())
        configFileName = configFileTokenizer.nextToken();
    }

    inputOperator.setMapClass(mapClass);
    inputOperator.setConfigFile(configFileName);
    inputOperator.setCombineClass(combineClass);

    ReduceOperator<K2, V2, K2, V2> reduceOpr = dag.addOperator("reduce", new ReduceOperator<K2, V2, K2, V2>());
    reduceOpr.setReduceClass(reduceClass);
    reduceOpr.setConfigFile(configFileName);
    dag.setAttribute(reduceOpr, Context.OperatorContext.INITIAL_PARTITION_COUNT, numberOfReducers);

    HdfsKeyValOutputOperator<K2,V2> console = dag.addOperator("console", new HdfsKeyValOutputOperator<K2,V2>());
    console.setFilePathPattern(outputDirName);
    // ConsoleOutputOperator console = dag.addOperator("console", new
    // ConsoleOutputOperator());

    dag.addStream("input_map", inputOperator.output, reduceOpr.input);
    dag.addStream("input_count_map", inputOperator.outputCount, reduceOpr.inputCount);

    dag.addStream("console_reduce", reduceOpr.output, console.input);

  }

}
