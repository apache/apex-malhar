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

package org.apache.apex.malhar.lib.fs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.lib.io.fs.AbstractSingleFileOutputOperator;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * This class is responsible for writing tuples to HDFS. All tuples are written
 * to the same file. Rolling file based on size, no. of tuples, idle windows,
 * elapsed windows is supported.
 * 
 * @param <T>
 */

class BytesFileOutputOperator extends AbstractSingleFileOutputOperator<byte[]>
{

  /**
   * Flag to mark if new data in current application window
   */
  private transient boolean isNewDataInCurrentWindow;

  /**
   * Separator between the tuples
   */
  private String tupleSeparator;

  /**
   * byte[] representation of tupleSeparator
   */
  private transient byte[] tupleSeparatorBytes;

  /**
   * No. of bytes received in current application window
   */
  @AutoMetric
  private long byteCount;

  /**
   * No. of tuples present in current part for file
   */
  private long currentPartTupleCount;

  /**
   * Max. number of tuples allowed per part. Part file will be finalized after
   * these many tuples
   */
  private long maxTupleCount = Long.MAX_VALUE;

  /**
   * No. of windows since last new data received
   */
  private long currentPartIdleWindows;

  /**
   * Max number of idle windows for which no new data is added to current part
   * file. Part file will be finalized after these many idle windows after last
   * new data.
   */
  private long maxIdleWindows = Long.MAX_VALUE;

  /**
   * Stream codec for string input port
   */
  protected StreamCodec<String> stringStreamCodec;

  /**
   * Default value for stream expiry
   */
  private static final long DEFAULT_STREAM_EXPIRY_ACCESS_MILL = 60 * 60 * 1000L; //1 hour

  /**
   * Default value for rotation windows
   */
  private static final int DEFAULT_ROTATION_WINDOWS = 2 * 60 * 10; //10 min  

  /**
   * Initializing default values for tuple separator, stream expiry, rotation
   * windows
   */
  public BytesFileOutputOperator()
  {
    setTupleSeparator(System.getProperty("line.separator"));
    setExpireStreamAfterAccessMillis(DEFAULT_STREAM_EXPIRY_ACCESS_MILL);
    setRotationWindows(DEFAULT_ROTATION_WINDOWS);
  }

  /**
   * Input port for receiving string tuples.
   */
  public final transient DefaultInputPort<String> stringInput = new DefaultInputPort<String>()
  {
    @Override
    public void process(String tuple)
    {
      processTuple(tuple.getBytes());
    }

    @Override
    public StreamCodec<String> getStreamCodec()
    {
      if (BytesFileOutputOperator.this.stringStreamCodec == null) {
        return super.getStreamCodec();
      } else {
        return stringStreamCodec;
      }
    }
  };

  /**
   * {@inheritDoc}
   * 
   * @return byte[] representation of the given tuple. if input tuple is of type
   *         byte[] then it is returned as it is. for any other type toString()
   *         representation is used to generate byte[].
   */
  @Override
  protected byte[] getBytesForTuple(byte[] tuple)
  {
    ByteArrayOutputStream bytesOutStream = new ByteArrayOutputStream();

    try {
      bytesOutStream.write(tuple);
      bytesOutStream.write(tupleSeparatorBytes);
      byteCount += bytesOutStream.size();
      return bytesOutStream.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        bytesOutStream.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Initializing per window level fields {@inheritDoc}
   */
  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    byteCount = 0;
    isNewDataInCurrentWindow = false;
  }

  /**
   * {@inheritDoc} Does additional state maintenance for rollover
   */
  @Override
  protected void processTuple(byte[] tuple)
  {
    super.processTuple(tuple);
    isNewDataInCurrentWindow = true;

    if (++currentPartTupleCount == maxTupleCount) {
      rotateCall(getPartitionedFileName());
    }
  }

  /**
   * {@inheritDoc} Does additional checks if file should be rolled over for this
   * window.
   */
  @Override
  public void endWindow()
  {
    super.endWindow();

    if (!isNewDataInCurrentWindow) {
      ++currentPartIdleWindows;
    } else {
      currentPartIdleWindows = 0;
    }

    if (checkEndWindowFinalization()) {
      rotateCall(getPartitionedFileName());
    }
  }

  /**
   * Rollover check at the endWindow
   */
  private boolean checkEndWindowFinalization()
  {
    if ((currentPartIdleWindows == maxIdleWindows)) {
      return true;
    }
    return false;
  }

  /**
   * {@inheritDoc} Handles file rotation along with exception handling
   * 
   * @param lastFile
   */
  protected void rotateCall(String lastFile)
  {
    try {
      this.rotate(lastFile);
      currentPartIdleWindows = 0;
      currentPartTupleCount = 0;
    } catch (IOException ex) {
      LOG.error("Exception in file rotation", ex);
      DTThrowable.rethrow(ex);
    } catch (ExecutionException ex) {
      LOG.error("Exception in file rotation", ex);
      DTThrowable.rethrow(ex);
    }
  }


  /**
   * @return Separator between the tuples
   */
  public String getTupleSeparator()
  {
    return tupleSeparator;
  }

  /**
   * @param separator
   *          Separator between the tuples
   */
  public void setTupleSeparator(String separator)
  {
    this.tupleSeparator = separator;
    this.tupleSeparatorBytes = separator.getBytes();
  }

  /**
   * @return max tuples in a part file
   */
  public long getMaxTupleCount()
  {
    return maxTupleCount;
  }

  /**
   * @param maxTupleCount
   *          max tuples in a part file
   */
  public void setMaxTupleCount(long maxTupleCount)
  {
    this.maxTupleCount = maxTupleCount;
  }

  /**
   * @return max number of idle windows for rollover
   */
  public long getMaxIdleWindows()
  {
    return maxIdleWindows;
  }

  /**
   * @param maxIdleWindows max number of idle windows for rollover
   */
  public void setMaxIdleWindows(long maxIdleWindows)
  {
    this.maxIdleWindows = maxIdleWindows;
  }

  private static final Logger LOG = LoggerFactory.getLogger(BytesFileOutputOperator.class);
}
