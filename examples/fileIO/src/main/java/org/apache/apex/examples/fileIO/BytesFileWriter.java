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

public class BytesFileWriter extends AbstractFileOutputOperator<byte[]>
{
  private static final transient Logger LOG = LoggerFactory.getLogger(BytesFileWriter.class);
  private static final char START_FILE = ThroughputBasedReader.START_FILE;
  private static final char FINISH_FILE = ThroughputBasedReader.FINISH_FILE;
  private String fileName; // current file name
  private boolean eof;
  private transient ArrayList<byte[]> savedLines = new ArrayList<>();

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
    if (START_FILE == tuple.charAt(0)) {
      // sanity check
      if (null != fileName) {
        throw new RuntimeException(String.format("Error: fileName = %s, expected null", fileName));
      }

      fileName = tuple.substring(1);
      if (!savedLines.isEmpty()) {
        LOG.debug("Processing {} saved lines", savedLines.size());
        for (byte[] line : savedLines) {
          processTuple(line);
        }
        savedLines.clear();
      }
      return;
    }

    final int last = tuple.length() - 1;
    if (FINISH_FILE == tuple.charAt(last)) { // end of file
      String name = tuple.substring(0, last);
      LOG.info("Closing file: " + name);
      if (null == fileName || !fileName.equals(name)) {
        throw new RuntimeException(String.format("Error: fileName = %s != %s = tuple", fileName, tuple));
      }
      eof = true;
      return;
    }
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
    requestFinalize(fileName);
    super.endWindow();

    eof = false;
    fileName = null;
  }

  @Override
  protected String getFileName(byte[] tuple)
  {
    return fileName;
  }

  @Override
  protected byte[] getBytesForTuple(byte[] tuple)
  {
    return tuple;
  }

  @Override
  public void processTuple(byte[] tuple)
  {
    if (null == fileName) {
      savedLines.add(tuple);
      return;
    }
    super.processTuple(tuple);
  }

}
