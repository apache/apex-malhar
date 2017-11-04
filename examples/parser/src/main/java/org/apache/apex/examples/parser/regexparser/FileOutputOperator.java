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

package org.apache.apex.examples.parser.regexparser;

import javax.validation.constraints.NotNull;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

/**
 * @since 3.8.0
 */
public class FileOutputOperator extends AbstractFileOutputOperator<Object>
{
  @NotNull
  private String outputFileName;

  @Override
  protected String getFileName(Object tuple)
  {
    return outputFileName;
  }

  @Override
  protected byte[] getBytesForTuple(Object tuple)
  {
    return (tuple.toString() + "\n").getBytes();
  }

  public String getOutputFileName()
  {
    return outputFileName;
  }

  public void setOutputFileName(String outputFileName)
  {
    this.outputFileName = outputFileName;
  }
}
