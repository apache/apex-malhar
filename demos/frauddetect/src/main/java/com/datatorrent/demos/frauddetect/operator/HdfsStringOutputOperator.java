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
package com.datatorrent.demos.frauddetect.operator;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAGContext;
import com.datatorrent.lib.io.fs.AbstractFSWriter;

import java.io.File;

/**
 * Adapter for writing Strings to HDFS
 * <p>
 * Serializes tuples into a HDFS file.<br/>
 * </p>
 *
 * @since 0.9.4
 */
public class HdfsStringOutputOperator extends AbstractFSWriter<String, String>
{
  private transient String outputFileName;
  private transient String contextId;
  private int index = 0;

  public HdfsStringOutputOperator()
  {
    setMaxLength(1024 * 1024);
  }

  @Override
  public void setup(OperatorContext context)
  {
    contextId = context.getValue(DAGContext.APPLICATION_NAME);
    outputFileName = File.separator + contextId +
                     File.separator + "transactions.out.part";
    super.setup(context);
  }

  @Override
  public byte[] getBytesForTuple(String t)
  {
    return t.getBytes();
  }

  @Override
  protected String getFileName(String tuple)
  {
    return outputFileName;
  }

  @Override
  public String getPartFileName(String fileName,
                                int part)
  {
    return fileName + part;
  }
}
