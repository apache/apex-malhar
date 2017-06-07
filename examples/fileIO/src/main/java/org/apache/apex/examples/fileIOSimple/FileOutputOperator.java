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

package org.apache.apex.examples.fileIOSimple;

import javax.validation.constraints.NotNull;

import com.datatorrent.api.Context;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

/**
 * Write incoming lines to output file
 */
public class FileOutputOperator extends AbstractFileOutputOperator<String>
{
  private static final String CHARSET_NAME = "UTF-8";
  private static final String NL = System.lineSeparator();

  @NotNull
  private String fileName;

  private transient String fName;    // per partition file name

  @Override
  public void setup(Context.OperatorContext context)
  {
    // create file name for this partition by appending the operator id to
    // the base name
    //
    long id = context.getId();
    fName = fileName + "_p" + id;
    super.setup(context);
  }

  @Override
  protected String getFileName(String tuple)
  {
    return fName;
  }

  @Override
  protected byte[] getBytesForTuple(String line)
  {
    byte[] result = null;
    try {
      result = (line + NL).getBytes(CHARSET_NAME);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  // getters and setters
  public String getFileName()
  {
    return fileName;
  }

  public void setFileName(String v)
  {
    fileName = v;
  }
}
