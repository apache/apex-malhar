/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io.fs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;

/**
 * Hdfs file output operator that writes the data from the tuple to the file supplied in the tuple.
 *
 * If output port is connected, it sends the output to the output port.
 * @param <INPUT> type of incoming tuple
 * @param <OUTPUT> type of outgoing tuple
 */
public abstract class AbstractHdfsTupleFileOutputOperator<INPUT, OUTPUT> extends AbstractHdfsFileOutputOperator<INPUT>
{
  /**
   *
   * @param context
   */
  @Override
  public void setup(Context.OperatorContext context)
  {
    try {
      fs = FileSystem.get(new Path(filePath).toUri(), new Configuration());
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  protected void processTuple(INPUT tuple)
  {
    try {
      Path filepath = new Path(filePath + "/" + getFileName(tuple));
      openFile(filepath);
      byte[] tupleBytes = getBytesForTuple(tuple);
      boolean written = false;
      if (bufferedOutput != null) {
        bufferedOutput.write(tupleBytes);
        written = true;
      }
      else if (fsOutput != null) {
        fsOutput.write(tupleBytes);
        written = true;
      }
      if (written) {
        totalBytesWritten += tupleBytes.length;
      }
      closeFile();
      if (output.isConnected()) {
        output.emit(getOutputTuple(tuple));
      }
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }

  }

  protected abstract String getFileName(INPUT t);

  protected abstract OUTPUT getOutputTuple(INPUT t);

  public final transient DefaultOutputPort<OUTPUT> output = new DefaultOutputPort<OUTPUT>();
  private static final long serialVersionUID = 201405151751L;
}
