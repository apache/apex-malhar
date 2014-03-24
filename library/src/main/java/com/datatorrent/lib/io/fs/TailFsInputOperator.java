/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.ActivationListener;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 * <p>
 * This operator implements "tail -f" command. If the operator has reached the end of the file, it will wait till more
 * data comes
 *
 * <br>
 * <b>Ports</b>:<br>
 * <b>outport</b>: emits &lt;String&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>filePath</b> : Path for file to be read. <br>
 * <b>delay</b>: Thread sleep interval after emitting line.<br>
 * <b>position</b>: The position from where to start reading the file.<br>
 * <b>numberOfTuples</b>: number of tuples to be emitted in a single emit Tuple call.<br>
 * <b>end</b>: if the user wants to start tailing from end.<br>
 * <br>
 *
 */

public class TailFsInputOperator implements InputOperator, ActivationListener<OperatorContext>
{
  /**
   * The file which will be tailed.
   */
  private String filePath;

  /**
   * This holds the position from which to start reading the stream
   */
  private long position;

  /**
   * The amount of time to wait for the file to be updated.
   */
  private long delay = 10;

  /**
   * The number of lines to emit in a single window
   */
  private int numberOfTuples = 1000;

  /**
   * Whether to tail from the end or start of file
   */
  private boolean end;

  /**
   * The delimiter used to identify tne end of the record
   */
  private char delimiter = '\n';

  /**
   * This is used to store the last access time of the file
   */
  private transient long accessTime;

  private transient RandomAccessFile reader;
  private transient File file;

  /**
   * @return the filePath
   */
  public String getFilePath()
  {
    return filePath;
  }

  /**
   * @param filePath
   *          the filePath to set
   */
  public void setFilePath(String filePath)
  {
    this.filePath = filePath;
  }

  /**
   * @return the delay
   */
  public long getDelay()
  {
    return delay;
  }

  /**
   * @param delay
   *          the delay to set
   */
  public void setDelay(long delay)
  {
    this.delay = delay;
  }

  /**
   * @return the end
   */
  public boolean isEnd()
  {
    return end;
  }

  /**
   * @param end
   *          the end to set
   */
  public void setEnd(boolean end)
  {
    this.end = end;
  }

  /**
   * @return the position
   */
  public long getPosition()
  {
    return position;
  }

  /**
   * @param position
   *          the position to set
   */
  public void setPosition(long position)
  {
    this.position = position;
  }

  /**
   * @return the numberOfTuples
   */
  public int getNumberOfTuples()
  {
    return numberOfTuples;
  }

  /**
   * @param numberOfTuples
   *          the numberOfTuples to set
   */
  public void setNumberOfTuples(int numberOfTuples)
  {
    this.numberOfTuples = numberOfTuples;
  }

  /**
   * @return the delimiter
   */
  public char getDelimiter()
  {
    return delimiter;
  }

  /**
   * @param delimiter
   *          the delimiter to set
   */
  public void setDelimiter(char delimiter)
  {
    this.delimiter = delimiter;
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
    try {
      this.position = reader.getFilePointer();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void activate(OperatorContext ctx)
  {
    try {
      file = new File(filePath);
      reader = new RandomAccessFile(file, "r");
      position = end ? file.length() : position;
      reader.seek(position);
      accessTime = System.currentTimeMillis();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void deactivate()
  {
    try {
      reader.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    position = 0;
  }

  @Override
  public void emitTuples()
  {
    int localCounter = numberOfTuples;
    try {
      while (localCounter >= 0) {
        String str = readLine();
        if (str == null) {
          //logger.debug("reached end of file");
        } else {
          output.emit(str);
        }
        try {
          Thread.sleep(delay);
        } catch (InterruptedException e) {
        }
        --localCounter;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String readLine() throws IOException
  {
    StringBuffer sb = new StringBuffer();
    char readChar;
    int ch;
    long pos = reader.getFilePointer();
    long length = file.length();
    if ((length < pos) || (length == pos && FileUtils.isFileNewer(file, accessTime))) {
      // file got rotated or truncated
      reader.close();
      reader = new RandomAccessFile(file, "r");
      position = 0;
      reader.seek(position);
      pos = 0;
    }
    accessTime = System.currentTimeMillis();
    while ((ch = reader.read()) != -1) {
      readChar = (char) ch;
      if (readChar != delimiter) {
        sb.append(readChar);
      } else {
        return sb.toString();
      }
    }
    reader.seek(pos);
    return null;
  }

  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

  private static final Logger logger = LoggerFactory.getLogger(TailFsInputOperator.class);

}
