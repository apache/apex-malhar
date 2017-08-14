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
package org.apache.apex.malhar.sql.operators;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.apex.malhar.lib.io.fs.AbstractFileInputOperator;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.DefaultOutputPort;

/**
 * This operator reads data from given file/folder in line by line fashion.
 *
 * @since 3.6.0
 */
@InterfaceStability.Evolving
public class LineReader extends AbstractFileInputOperator<byte[]>
{
  public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<>();

  protected transient BufferedReader br;

  @Override
  protected InputStream openFile(Path path) throws IOException
  {
    InputStream is = super.openFile(path);
    br = new BufferedReader(new InputStreamReader(is));
    return is;
  }

  @Override
  protected void closeFile(InputStream is) throws IOException
  {
    super.closeFile(is);
    br.close();
    br = null;
  }

  @Override
  protected byte[] readEntity() throws IOException
  {
    String s = br.readLine();
    if (s != null) {
      return s.getBytes();
    }
    return null;
  }

  @Override
  protected void emit(byte[] tuple)
  {
    output.emit(tuple);
  }
}
