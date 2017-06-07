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

package org.apache.apex.examples.fileIOMultiDir;

import org.apache.hadoop.conf.Configuration;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import static com.datatorrent.api.Context.PortContext.PARTITION_PARALLEL;

@ApplicationAnnotation(name = "FileIO")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // create operators
    FileReader reader = dag.addOperator("read",  FileReader.class);
    FileWriter writer = dag.addOperator("write", FileWriter.class);

    reader.setScanner(new FileReaderMultiDir.SlicedDirectoryScanner());

    // using parallel partitioning ensures that lines from a single file are handled
    // by the same writer
    //
    dag.setInputPortAttribute(writer.input, PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(writer.control, PARTITION_PARALLEL, true);

    dag.addStream("data", reader.output, writer.input);
    dag.addStream("ctrl", reader.control, writer.control);
  }
}
