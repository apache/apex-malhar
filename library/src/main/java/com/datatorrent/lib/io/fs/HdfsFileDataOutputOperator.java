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

import java.util.Date;

import com.datatorrent.lib.io.fs.HdfsFileDataOutputOperator.FileData;
import com.datatorrent.lib.io.fs.HdfsFileDataOutputOperator.FileData.FileInfo;

/**
 * Hdfs output operator that takes {@link FileData} input tuple and writes data to the file specified in the tuple.
 */
public class HdfsFileDataOutputOperator extends AbstractHdfsTupleFileOutputOperator<FileData, FileData.FileInfo>
{
  @Override
  protected String getFileName(FileData t)
  {
    return t.info.name;
  }

  @Override
  protected FileInfo getOutputTuple(FileData t)
  {
    return t.info;
  }

  @Override
  protected byte[] getBytesForTuple(FileData t)
  {
    return t.data.getBytes();
  }

  public static class FileData
  {
    public static class FileInfo
    {
      public String name;
    }

    public FileInfo info = new FileInfo();
    public String data;
  }

  private static final long serialVersionUID = 201405151847L;
}
