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

package com.datatorrent.lib.io.fs;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.lib.io.block.AbstractBlockReader.ReaderRecord;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.fs.AbstractFileSplitter.FileMetadata;
import com.datatorrent.netlet.util.Slice;

/**
 * HDFS file copy module can be used in conjunction with file input modules to
 * copy files from any file system to HDFS. This module supports parallel write
 * to multiple blocks of the same file and then stitching those blocks in
 * original sequence.
 * 
 * Essential operators are wrapped into single component using Module API.
 * 
 */
public class HDFSFileCopyModule implements Module
{

  /**
   * Path of the output directory. Relative path of the files copied will be
   * maintained w.r.t. source directory and output directory
   */
  @NotNull
  protected String outputDirectoryPath;

  /**
   * Flag to control if existing file with same name should be overwritten
   */
  private boolean overwriteOnConflict;

  /**
   * Input port for files metadata.
   */
  public final transient ProxyInputPort<FileMetadata> filesMetadataInput = new ProxyInputPort<FileMetadata>();

  /**
   * Input port for blocks metadata
   */
  public final transient ProxyInputPort<BlockMetadata.FileBlockMetadata> blocksMetadataInput = new ProxyInputPort<BlockMetadata.FileBlockMetadata>();

  /**
   * Input port for blocks data
   */
  public final transient ProxyInputPort<ReaderRecord<Slice>> blockData = new ProxyInputPort<ReaderRecord<Slice>>();

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    //Defining DAG
    BlockWriter blockWriter = dag.addOperator("BlockWriter", new BlockWriter());
    Synchronizer synchronizer = dag.addOperator("BlockSynchronizer", new Synchronizer());

    dag.setInputPortAttribute(blockWriter.input, PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(blockWriter.blockMetadataInput, PortContext.PARTITION_PARALLEL, true);
    dag.addStream("CompletedBlockmetadata", blockWriter.blockMetadataOutput, synchronizer.blocksMetadataInput);

    HDFSFileMerger merger = new HDFSFileMerger();
    merger = dag.addOperator("FileMerger", merger);
    dag.addStream("MergeTrigger", synchronizer.trigger, merger.input);

    //DevNull<OutputFileMetadata> devNull1 = dag.addOperator("devNull1", DevNull.class);
    //dag.addStream("ignored", merger.completedFilesMetaOutput, devNull1.data);

    //Setting operator properties
    merger.setFilePath(outputDirectoryPath);
    merger.setOverwriteOnConflict(overwriteOnConflict);

    //Binding proxy ports
    filesMetadataInput.set(synchronizer.filesMetadataInput);
    blocksMetadataInput.set(blockWriter.blockMetadataInput);
    blockData.set(blockWriter.input);

  }

  public String getOutputDirectoryPath()
  {
    return outputDirectoryPath;
  }

  public void setOutputDirectoryPath(String outputDirectoryPath)
  {
    this.outputDirectoryPath = outputDirectoryPath;
  }

  public boolean isOverwriteOnConflict()
  {
    return overwriteOnConflict;
  }

  public void setOverwriteOnConflict(boolean overwriteOnConflict)
  {
    this.overwriteOnConflict = overwriteOnConflict;
  }

}
