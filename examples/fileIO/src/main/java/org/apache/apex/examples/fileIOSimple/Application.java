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

package org.apache.apex.examples.fileIOSimple;

import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * Simple application illustrating file input-output
 */
@ApplicationAnnotation(name = "SimpleFileIO")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // create operators
    LineByLineFileInputOperator in = dag.addOperator("input",
        new LineByLineFileInputOperator());
    FileOutputOperator out = dag.addOperator("output",
        new FileOutputOperator());
    // configure operators
    in.setDirectory("/tmp/SimpleFileIO/input-dir");
    out.setFilePath("/tmp/SimpleFileIO/output-dir");
    out.setMaxLength(1_000_000);        // file rotation size

    // create streams
    dag.addStream("data", in.output, out.input);
  }
}
