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

package org.apache.apex.malhar.lib.fs;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;

/**
 * This operator writes incoming tuples to files.
 * MetaData about the files is emitted on the output port for downstream processing (if any)
 *
 * @param <INPUT>
 *          Type for incoming tuples. Converter needs to be defined which
 *          converts these tuples to byte[]. Default converters for String,
 *          byte[] tuples are provided in S3TupleOutputModule.
 *
 * @since 3.7.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class FSRecordCompactionOperator<INPUT> extends GenericFileOutputOperator<INPUT>
{

  /**
   * Output port for emitting metadata for finalized files.
   */
  public transient DefaultOutputPort<OutputMetaData> output = new DefaultOutputPort<OutputMetaData>();

  /**
   * Queue for holding finalized files for emitting on output port
   */
  private Queue<OutputMetaData> emitQueue = new LinkedBlockingQueue<OutputMetaData>();

  @NotNull
  String outputDirectoryName = "COMPACTION_OUTPUT_DIR";

  @NotNull
  String outputFileNamePrefix = "tuples-";

  public FSRecordCompactionOperator()
  {
    filePath = "";
    outputFileName = outputFileNamePrefix;
    maxLength = 128 * 1024 * 1024L;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    filePath = context.getValue(DAG.APPLICATION_PATH) + Path.SEPARATOR + outputDirectoryName;
    outputFileName = outputFileNamePrefix + context.getValue(DAG.APPLICATION_ID);
    super.setup(context);
  }

  @Override
  protected void finalizeFile(String fileName) throws IOException
  {
    super.finalizeFile(fileName);

    String src = filePath + Path.SEPARATOR + fileName;
    Path srcPath = new Path(src);
    long offset = fs.getFileStatus(srcPath).getLen();

    //Add finalized files to the queue
    OutputMetaData metaData = new OutputMetaData(src, fileName, offset);
    //finalizeFile is called from committed callback.
    //Tuples should be emitted only between beginWindow to endWindow. Thus using emitQueue.
    emitQueue.add(metaData);
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    //Emit finalized files from the queue
    while (!emitQueue.isEmpty()) {
      output.emit(emitQueue.poll());
    }
  }

  public String getOutputDirectoryName()
  {
    return outputDirectoryName;
  }

  public void setOutputDirectoryName(@NotNull String outputDirectoryName)
  {
    this.outputDirectoryName = Preconditions.checkNotNull(outputDirectoryName);
  }

  public String getOutputFileNamePrefix()
  {
    return outputFileNamePrefix;
  }

  public void setOutputFileNamePrefix(@NotNull String outputFileNamePrefix)
  {
    this.outputFileNamePrefix = Preconditions.checkNotNull(outputFileNamePrefix);
  }

  /**
   * Metadata for output file for downstream processing
   */
  public static class OutputMetaData
  {
    private String path;
    private String fileName;
    private long size;

    public OutputMetaData()
    {
    }

    public OutputMetaData(String path, String fileName, long size)
    {
      this.path = path;
      this.fileName = fileName;
      this.size = size;
    }

    public String getPath()
    {
      return path;
    }

    public void setPath(String path)
    {
      this.path = path;
    }

    public String getFileName()
    {
      return fileName;
    }

    public void setFileName(String fileName)
    {
      this.fileName = fileName;
    }

    public long getSize()
    {
      return size;
    }

    public void setSize(long size)
    {
      this.size = size;
    }
  }

}
