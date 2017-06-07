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

package org.apache.apex.examples.fileIO;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

/**
 * Write incoming line to output file
 */
public class FileWriter extends AbstractFileOutputOperator<String>
{
  private static final Logger LOG = LoggerFactory.getLogger(FileWriter.class);
  private static final String CHARSET_NAME = "UTF-8";
  private static final String NL = System.lineSeparator();
  private static final char START_FILE = FileReader.START_FILE;
  private static final char FINISH_FILE = FileReader.FINISH_FILE;

  private String fileName;    // current file name

  private boolean eof;

  // lines that arrive before the start control tuple are saved here
  private ArrayList<String> savedLines = new ArrayList<>();

  /**
   * control port for file start/finish control tuples
   */
  public final transient DefaultInputPort<String> control = new DefaultInputPort<String>()
  {
    @Override
    public void process(String tuple)
    {
      processControlTuple(tuple);
    }
  };

  private void processControlTuple(final String tuple)
  {
    if (START_FILE == tuple.charAt(0)) {                          // start of file
      LOG.debug("start tuple = {}", tuple);

      // sanity check
      if (null != fileName) {
        throw new RuntimeException(String.format("Error: fileName = %s, expected null", fileName));
      }

      fileName = tuple.substring(1);

      // if we have saved lines, process them
      if (!savedLines.isEmpty()) {
        LOG.debug("Processing {} saved lines", savedLines.size());
        for (String line : savedLines) {
          processTuple(line);
        }
        savedLines.clear();
      }

      return;
    }

    final int last = tuple.length() - 1;
    if (FINISH_FILE == tuple.charAt(last)) {        // end of file
      LOG.debug("finish tuple = {}", tuple);
      String name = tuple.substring(0, last);

      // sanity check : should match what we got with start control tuple
      if (null == fileName || !fileName.equals(name)) {
        throw new RuntimeException(String.format("Error: fileName = %s != %s = tuple", fileName, tuple));
      }

      eof = true;
      return;
    }

    // should never happen
    throw new RuntimeException("Error: Bad control tuple: {}" + tuple);
  }

  @Override
  public void processTuple(String tuple)
  {
    if (null == fileName) {
      savedLines.add(tuple);
      return;
    }

    super.processTuple(tuple);
  }

  @Override
  public void endWindow()
  {
    if (!eof) {
      return;
    }

    // got an EOF, so must have a file name
    if (null == fileName) {
      throw new RuntimeException("Error: fileName empty");
    }

    LOG.info("requesting finalize of {}", fileName);
    requestFinalize(fileName);
    super.endWindow();

    eof = false;
    fileName = null;
  }

  @Override
  protected String getFileName(String tuple)
  {
    return fileName;
  }

  @Override
  protected byte[] getBytesForTuple(String line)
  {
    LOG.debug("getBytesForTuple: line.length = {}", line.length());

    byte[] result = null;
    try {
      result = (line + NL).getBytes(CHARSET_NAME);
    } catch (Exception e) {
      LOG.info("Error: got exception {}", e);
      throw new RuntimeException(e);
    }
    return result;
  }

}
