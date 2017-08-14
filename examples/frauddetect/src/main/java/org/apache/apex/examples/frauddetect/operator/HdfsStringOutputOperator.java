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
package org.apache.apex.examples.frauddetect.operator;

import java.io.File;

import org.apache.apex.malhar.lib.io.fs.AbstractFileOutputOperator;

import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;

/**
 * Adapter for writing Strings to HDFS
 * <p>
 * Serializes tuples into a HDFS file.<br/>
 * </p>
 *
 * @since 0.9.4
 */
public class HdfsStringOutputOperator extends AbstractFileOutputOperator<String>
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
  public String getPartFileName(String fileName, int part)
  {
    return fileName + part;
  }
}
