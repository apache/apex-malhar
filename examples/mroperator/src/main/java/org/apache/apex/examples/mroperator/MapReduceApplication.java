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
package org.apache.apex.examples.mroperator;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;


/**
 * <p>
 * Abstract MapReduceApplication class.
 * </p>
 *
 * @since 0.9.0
 */
@ApplicationAnnotation(name = "MapReduceExample")
public abstract class MapReduceApplication<K1, V1, K2, V2> implements StreamingApplication
{
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


  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    String configurationFilePath = conf.get(this.getClass().getSimpleName() + ".configFile", "");

    MapOperator<K1, V1, K2, V2> inputOperator = dag.addOperator("Mapper", new MapOperator<K1, V1, K2, V2>());
    inputOperator.setInputFormatClass(inputFormat);

    String configFileName = null;
    if (configurationFilePath != null && !configurationFilePath.isEmpty()) {
      StringTokenizer configFileTokenizer = new StringTokenizer(configurationFilePath, "/");
      configFileName = configFileTokenizer.nextToken();
      while (configFileTokenizer.hasMoreTokens()) {
        configFileName = configFileTokenizer.nextToken();
      }
    }

    inputOperator.setMapClass(mapClass);
    inputOperator.setConfigFile(configFileName);
    inputOperator.setCombineClass(combineClass);

    ReduceOperator<K2, V2, K2, V2> reduceOpr = dag.addOperator("Reducer", new ReduceOperator<K2, V2, K2, V2>());
    reduceOpr.setReduceClass(reduceClass);
    reduceOpr.setConfigFile(configFileName);

    HdfsKeyValOutputOperator<K2, V2> console = dag.addOperator("Console", new HdfsKeyValOutputOperator<K2, V2>());

    dag.addStream("Mapped-Output", inputOperator.output, reduceOpr.input);
    dag.addStream("Mapper-Count", inputOperator.outputCount, reduceOpr.inputCount);
    dag.addStream("Reduced-Output", reduceOpr.output, console.input);
  }
}
