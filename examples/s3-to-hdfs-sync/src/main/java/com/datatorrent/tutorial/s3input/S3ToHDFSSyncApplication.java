package com.datatorrent.tutorial.s3input;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.fs.HDFSFileCopyModule;
import com.datatorrent.lib.io.fs.S3InputModule;

/**
 * Simple application illustrating file copy from S3
 */
@ApplicationAnnotation(name="S3-to-HDFS-Sync")
public class S3ToHDFSSyncApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    S3InputModule inputModule = dag.addModule("S3InputModule", new S3InputModule());
    HDFSFileCopyModule outputModule = dag.addModule("HDFSFileCopyModule", new HDFSFileCopyModule());

    dag.addStream("FileMetaData", inputModule.filesMetadataOutput, outputModule.filesMetadataInput);
    dag.addStream("BlocksMetaData", inputModule.blocksMetadataOutput, outputModule.blocksMetadataInput)
        .setLocality(Locality.THREAD_LOCAL);
    dag.addStream("BlocksData", inputModule.messages, outputModule.blockData).setLocality(Locality.THREAD_LOCAL);
  }

}
