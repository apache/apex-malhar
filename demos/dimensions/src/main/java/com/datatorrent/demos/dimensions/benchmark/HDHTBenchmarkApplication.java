/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.dimensions.benchmark;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.hdht.HDHTWriter;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.file.tfile.DTFile;

@ApplicationAnnotation(name="HDHTBenchmarkApplication")
public class HDHTBenchmarkApplication implements StreamingApplication
{
  @Override public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.APPLICATION_NAME, "HDHTBenchmarkApplication");
    Generator gen = dag.addOperator("Generator", new Generator());
    gen.setTupleBlast(1000);
    gen.setSleepms(0);
    dag.getOperatorMeta("Generator").getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 1);

    HDSOperator hdsOut = dag.addOperator("Store", new HDSOperator());
    TFileImpl.DTFileImpl hdsFile = new TFileImpl.DTFileImpl();
    hdsFile.setBasePath("WALBenchMarkDir");
    hdsOut.setFileStore(hdsFile);
    dag.getOperatorMeta("Store").getAttributes().put(Context.OperatorContext.COUNTERS_AGGREGATOR, new HDHTWriter.BucketIOStatAggregator());

    dag.addStream("s1", gen.out, hdsOut.input).setLocality(DAG.Locality.THREAD_LOCAL);
  }
}
