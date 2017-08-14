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

package org.apache.apex.examples.csvformatter;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.io.fs.AbstractFileOutputOperator;

import com.datatorrent.api.Context.OperatorContext;

/**
 * HDFSoutput operator with implementation to write Objects to HDFS
 *
 * @param <T>
 *
 * @since 3.7.0
 */
public class HDFSOutputOperator<T> extends AbstractFileOutputOperator<T>
{

  @NotNull
  String outFileName;

  //setting default value
  String lineDelimiter = "\n";

  //Switch to write the files to HDFS - set to false to diable writes
  private boolean writeFilesFlag = true;

  int id;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    id = context.getId();
  }

  public boolean isWriteFilesFlag()
  {
    return writeFilesFlag;
  }

  public void setWriteFilesFlag(boolean writeFilesFlag)
  {
    this.writeFilesFlag = writeFilesFlag;
  }

  public String getOutFileName()
  {
    return outFileName;
  }

  public void setOutFileName(String outFileName)
  {
    this.outFileName = outFileName;
  }

  @Override
  protected String getFileName(T tuple)
  {
    return getOutFileName() + id;
  }

  public String getLineDelimiter()
  {
    return lineDelimiter;
  }

  public void setLineDelimiter(String lineDelimiter)
  {
    this.lineDelimiter = lineDelimiter;
  }

  @Override
  protected byte[] getBytesForTuple(T tuple)
  {
    String temp = tuple.toString().concat(String.valueOf(lineDelimiter));
    byte[] theByteArray = temp.getBytes();

    return theByteArray;
  }

  @Override
  protected void processTuple(T tuple)
  {
    //if (writeFilesFlag) {
    //}
    super.processTuple(tuple);
  }

}
