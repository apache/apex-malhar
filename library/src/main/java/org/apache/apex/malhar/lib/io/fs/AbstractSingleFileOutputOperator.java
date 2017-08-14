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
package org.apache.apex.malhar.lib.io.fs;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang.StringUtils;

import com.datatorrent.api.Context.OperatorContext;

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

  /**
   * partitionedFileName string format specifier
      e.g. fileName_physicalPartionId -> %s_%d
   */
  private String partitionedFileNameformat = "%s_%d";

  /**
   * Derived name for file based on physicalPartitionId
   */
  private transient String partitionedFileName;

  /**
   * Physical partition id for the current partition.
   */
  private transient int physicalPartitionId;

  /**
   * Initializing current partition id, partitionedFileName etc. {@inheritDoc}
   */
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    physicalPartitionId = context.getId();
    if (StringUtils.isEmpty(partitionedFileNameformat)) {
      partitionedFileName = outputFileName;
    } else {
      partitionedFileName = String.format(partitionedFileNameformat, outputFileName, physicalPartitionId);
    }
  }

  @Override
  protected String getFileName(INPUT tuple)
  {
    return partitionedFileName;
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

  /**
   * @return string format specifier for the partitioned file name
   */
  public String getPartitionedFileNameformat()
  {
    return partitionedFileNameformat;
  }

  /**
   * @param partitionedFileNameformat
   *          string format specifier for the partitioned file name. It should have one %s and one %d.
   *          e.g. fileName_physicalPartionId -> %s_%d
   */
  public void setPartitionedFileNameformat(String partitionedFileNameformat)
  {
    this.partitionedFileNameformat = partitionedFileNameformat;
  }

  /**
   * @return
   * Derived name for file based on physicalPartitionId
   */
  public String getPartitionedFileName()
  {
    return partitionedFileName;
  }

}
