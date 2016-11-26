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
package org.apache.apex.malhar.sql.sample;

import java.io.File;
import java.io.IOException;

import org.apache.apex.malhar.sql.SQLExecEnvironment;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "SQLApplicationWithModelFile")
/**
 * @since 3.6.0
 */
public class SQLApplicationWithModelFile implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    String modelFile = conf.get("modelFile");
    String model;
    try {
      model = FileUtils.readFileToString(new File(modelFile));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    SQLExecEnvironment.getEnvironment()
        .withModel(model)
        .executeSQL(dag, conf.get("sql"));
  }
}
