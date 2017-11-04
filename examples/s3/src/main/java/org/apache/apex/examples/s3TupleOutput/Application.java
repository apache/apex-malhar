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
package org.apache.apex.examples.s3TupleOutput;

import org.apache.apex.malhar.lib.fs.FSRecordReaderModule;
import org.apache.apex.malhar.lib.fs.s3.S3TupleOutputModule.S3BytesOutputModule;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * Simple application illustrating file copy from S3
 *
 * @since 3.8.0
 */
@ApplicationAnnotation(name = "s3-output-line")
public class Application implements StreamingApplication
{

  public void populateDAG(DAG dag, Configuration conf)
  {
    FSRecordReaderModule recordReader = dag.addModule("lineInput", FSRecordReaderModule.class);
    S3BytesOutputModule s3StringOutputModule = dag.addModule("s3TupleOutput", S3BytesOutputModule.class);
    dag.addStream("data", recordReader.records, s3StringOutputModule.input);

  }

}
