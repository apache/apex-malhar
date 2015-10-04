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

/**
 * This is a simple class that output all tuples to a single file.
 *
 * @displayName FS Single File Writer
 * @category Output
 * @tags fs, file, output operator
 *
 * @param <INPUT> The type of the incoming tuples.
 *
 * @since 2.0.0
 */
public abstract class AbstractSingleFileOutputOperator<INPUT> extends AbstractFileOutputOperator<INPUT>
{
  /**
   * The name of the output file to write to.
   */
  @NotNull
  protected String outputFileName;

  @Override
  protected String getFileName(INPUT tuple)
  {
    return outputFileName;
  }

  /**
   * Sets the name for the output file.
   * @param outputFileName The full path for the output file.
   */
  public void setOutputFileName(String outputFileName)
  {
    this.outputFileName = outputFileName;
  }

  /**
   * Gets the full path for the output file.
   * @return The full path for the output file.
   */
  public String getOutputFileName()
  {
    return outputFileName;
  }
}
