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
package com.datatorrent.demos.wordcount;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;

// reads lines from input file and returns them; if end-of-file is reached, a control tuple
// is emitted on the control port
//
public class LineReader extends AbstractFileInputOperator<String>
{
  private static final Logger LOG = LoggerFactory.getLogger(LineReader.class);

  public final transient DefaultOutputPort<String> output  = new DefaultOutputPort<>();

  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<String> control = new DefaultOutputPort<>();

  private transient BufferedReader br = null;

  private Path path;

  @Override
  protected InputStream openFile(Path curPath) throws IOException
  {
    LOG.info("openFile: curPath = {}", curPath);
    path = curPath;
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
    path = null;
  }

  // return empty string 
  @Override
  protected String readEntity() throws IOException
  {
    // try to read a line
    final String line = br.readLine();
    if (null != line) {    // common case
      LOG.debug("readEntity: line = {}", line);
      return line;
    }

    // end-of-file; send control tuple, containing only the last component of the path
    // (only file name) on control port
    //
    if (control.isConnected()) {
      LOG.info("readEntity: EOF for {}", path);
      final String name = path.getName();    // final component of path
      control.emit(name);
    }

    return null;
  }

  @Override
  protected void emit(String tuple)
  {
    output.emit(tuple);
  }
}
