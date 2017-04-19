package com.example.s3output;

import org.apache.apex.malhar.lib.fs.s3.S3OutputModule;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.fs.FSInputModule;

/**
 * Application illustrating copy files from HDFS to S3 bucket.
 */
@ApplicationAnnotation(name="HDFSToS3App")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FSInputModule inputModule = dag.addModule("HDFSInputModule", new FSInputModule());
    S3OutputModule outputModule = dag.addModule("S3OutputModule", new S3OutputModule());

    dag.addStream("FileMetaData", inputModule.filesMetadataOutput, outputModule.filesMetadataInput);
    dag.addStream("BlocksMetaData", inputModule.blocksMetadataOutput, outputModule.blocksMetadataInput)
      .setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("BlocksData", inputModule.messages, outputModule.blockData).setLocality(DAG.Locality.CONTAINER_LOCAL);
  }
}
