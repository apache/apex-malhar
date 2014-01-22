/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io.fs;

import java.io.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;

/**
 * <p>
 * This operator opens given file from local file system. Each line is emitted on
 * output port, Thread waits for sleep interval after emitting line.
 *
 * <br>
 * <b>Ports</b>:<br>
 * <b>outport</b>: emits &lt;String&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>filePath</b> : Path for file to be read. <br>
 * <b>sleepInterval</b>: Thread sleep interval after emitting line.<br>
 * <br>
 *
 * @since 0.3.2
 */
public class LocalFsInputOperator extends AbstractLocalFSInputOperator
{
  public final transient DefaultOutputPort<String> outport = new DefaultOutputPort<String>();
  private DataInputStream in;
  private BufferedReader br;
  private int sleepInterval;

  public LocalFsInputOperator()
  {
    this.sleepInterval = 0;
  }

  @Override
  public void activate(OperatorContext context)
  {
    super.activate(context);
    in = new DataInputStream(input);
    br = new BufferedReader(new InputStreamReader(in));
  }

  @Override
  public void deactivate()
  {
    try {
      br.close();
    }
    catch (IOException ex) {
      logger.warn("Exception while closing the stream", ex);
    }

    super.deactivate();
  }

  @Override
  public void emitTuples(FileInputStream stream)
  {
    try {
      String strLine = br.readLine();
      if (strLine != null) {
        outport.emit(strLine);
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (sleepInterval > 0) {
      try {
        Thread.sleep(sleepInterval);
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public int getSleepInterval()
  {
    return sleepInterval;
  }

  public void setSleepInterval(int sleepInterval)
  {
    this.sleepInterval = sleepInterval;
  }

  private static final Logger logger = LoggerFactory.getLogger(LocalFsInputOperator.class);
}
