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
package org.apache.apex.malhar.lib.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 3.4.0
 */
public class IOUtils
{
  /**
   * Utility method to copy partial data from input stream to output stream.<br/>
   * This can be deprecated when we can move to a newer version of hadoop common.
   *
   * @param inputStream        input stream
   * @param length             length of bytes from the start of input stream
   * @param outputStream       output stream
   * @throws IOException
   */
  public static void copyPartial(InputStream inputStream, long length, OutputStream outputStream)
      throws IOException
  {
    copyPartial(inputStream, 0, length, outputStream);
  }

  /**
   * Utility method to copy partial data from input stream to output stream.<br/>
   * This can be deprecated when we can move to a newer version of hadoop common.
   *
   * @param inputStream   inputStream
   * @param fromOffset    offset in the input stream to copy data from
   * @param length        length of bytes in the input stream
   * @param outputStream  output stream
   * @throws IOException
   */
  public static void copyPartial(InputStream inputStream, long fromOffset, long length, OutputStream outputStream)
      throws IOException
  {
    if (fromOffset > 0) {
      long skippedBytes = inputStream.skip(fromOffset);
      if (skippedBytes < fromOffset) {
        LOGGER.debug("unable to skip bytes {} {}", skippedBytes, fromOffset);
      }
    }
    byte[] buffer;
    if (length < COPY_BUFFER_SIZE) {
      buffer = new byte[(int)length];
    } else {
      buffer = new byte[COPY_BUFFER_SIZE];
    }

    int bytesRead = 0;

    while (bytesRead <  length) {
      long remainingBytes = length - bytesRead;
      int bytesToWrite = remainingBytes < COPY_BUFFER_SIZE ? (int)remainingBytes : COPY_BUFFER_SIZE;
      bytesRead += inputStream.read(buffer);
      outputStream.write(buffer, 0, bytesToWrite);
    }
  }

  private static final int COPY_BUFFER_SIZE = 1024;
  private static final Logger LOGGER = LoggerFactory.getLogger(IOUtils.class);

}
