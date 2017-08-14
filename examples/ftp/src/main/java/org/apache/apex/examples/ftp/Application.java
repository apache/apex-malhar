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
package org.apache.apex.examples.ftp;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator.StringFileOutputOperator;
import org.apache.apex.malhar.lib.io.AbstractFTPInputOperator.FTPStringInputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * This application demonstrates the FTPStringInputOperator. It uses the FTPStringInputOperator which reads
 * data from a directory on an FTP server, and then writes it to a file using the  StringFileOutputOperator.
 *
 * @since 3.8.0
 */
@ApplicationAnnotation(name = "FTPInputExample")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // ftp read operator. Configuration through resources/META-INF/properties.xml
    FTPStringInputOperator reader = dag.addOperator("Reader", new FTPStringInputOperator());
    //Set properties for the FTP input operator
    reader.setHost("localhost");
    reader.setUserName("ftp");
    reader.setDirectory("sourceDir");
    reader.setPartitionCount(2);

    // writer that writes strings to a file on hdfs
    StringFileOutputOperator writer = dag.addOperator("Writer", new StringFileOutputOperator());
    //Set properties for the output operator
    writer.setFilePath("malhar_examples/ftp");
    writer.setFilePath("destination");

    //Connect reader output to writer
    dag.addStream("data", reader.output, writer.input);
  }

}
