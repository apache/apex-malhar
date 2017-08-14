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
package org.apache.apex.examples.wordcount;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.io.fs.AbstractFileInputOperator;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * Reads lines from input file and returns them. If EOF is reached, a control tuple
 * is emitted on the control port
 *
 * @since 3.2.0
 */
public class LineReader extends AbstractFileInputOperator<String>
{
  private static final Logger LOG = LoggerFactory.getLogger(LineReader.class);
  private boolean flag = true;

  /**
   * Output port on which lines from current file name are emitted
   */
  public final transient DefaultOutputPort<String> output  = new DefaultOutputPort<>();

  /**
   * Control port on which the current file name is emitted to indicate EOF
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<String> control = new DefaultOutputPort<>();

  private transient BufferedReader br = null;

  private Path path;

  /**
   * File open callback; wrap the file input stream in a buffered reader for reading lines
   * @param curPath The path to the file just opened
   */
  @Override
  protected InputStream openFile(Path curPath) throws IOException
  {
    LOG.info("openFile: curPath = {}", curPath);
    path = curPath;
    InputStream is = super.openFile(path);
    br = new BufferedReader(new InputStreamReader(is));
    return is;
  }

  /**
   * File close callback; close buffered reader
   * @param is File input stream that will imminently be closed
   */
  @Override
  protected void closeFile(InputStream is) throws IOException
  {
    super.closeFile(is);
    br.close();
    br = null;
    path = null;
  }

  /**
   * {@inheritDoc}
   * If we hit EOF, emit file name on control port
   */
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
      flag = false;
      control.emit(name);
    }

    return null;
  }

  @Override
  public void emitTuples()
  {
    if (flag) {
      super.emitTuples();
    }
  }

  @Override
  protected void emit(String tuple)
  {
    output.emit(tuple);
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    flag = true;
  }
}
