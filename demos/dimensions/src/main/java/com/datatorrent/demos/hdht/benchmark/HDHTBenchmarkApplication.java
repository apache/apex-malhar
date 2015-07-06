/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.hdht.benchmark;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.hdht.HDHTWriter;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;

import org.apache.hadoop.conf.Configuration;

/**
 * HDHTBenchmarkApplication
 *
 * @since 2.0.0
 */
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
