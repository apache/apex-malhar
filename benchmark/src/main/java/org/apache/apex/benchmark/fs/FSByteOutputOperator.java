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
package org.apache.apex.benchmark.fs;

import java.util.Arrays;

import javax.validation.constraints.Min;

import org.apache.apex.malhar.lib.io.fs.AbstractFileOutputOperator;

/**
 * This output operator receives
 *
 * @since 0.9.4
 */
public class FSByteOutputOperator extends AbstractFileOutputOperator<byte[]>
{
  /**
   * The number of unique files to output tuples to.
   */
  @Min(1)
  private int outputFileCount = 1;

  /**
   * The file a tuple is written out to is determined by modding the hashcode of the
   * tuple by the outputFileCount.
   *
   * @param tuple The input tuple to write out.
   * @return The name of the file to write the tuple to.
   */
  @Override
  protected String getFileName(byte[] tuple)
  {
    return ((Integer)(Arrays.hashCode(tuple) % outputFileCount)).toString();
  }

  @Override
  protected byte[] getBytesForTuple(byte[] tuple)
  {
    for (int counter = 0;
        counter < tuple.length;
        counter++) {
      tuple[counter] += 1;
    }

    return tuple;
  }

  public void setOutputFileCount(int outputFileCount)
  {
    this.outputFileCount = outputFileCount;
  }

  public int getOutputFileCount()
  {
    return outputFileCount;
  }
}
