package com.datatorrent.tutorials.s3output;

import org.apache.apex.malhar.lib.fs.FSRecordReaderModule;
import org.apache.apex.malhar.lib.fs.s3.S3TupleOutputModule.S3BytesOutputModule;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * Simple application illustrating file copy from S3
 */
@ApplicationAnnotation(name="s3-output-line")
public class Application implements StreamingApplication
{

  public void populateDAG(DAG dag, Configuration conf)
  {
    FSRecordReaderModule recordReader = dag.addModule("lineInput", FSRecordReaderModule.class);
    S3BytesOutputModule s3StringOutputModule = dag.addModule("s3output", S3BytesOutputModule.class);
    dag.addStream("data", recordReader.records, s3StringOutputModule.input);
    
  }

}
