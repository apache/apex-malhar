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
package org.apache.hadoop.io.file.tfile;

import java.io.ByteArrayInputStream;

/**
 * A reusable ByteArrayInputStream extends {@link ByteArrayInputStream} to avoid creating stream object on same byte array.
 * <br><br>Call renew() method to reuse this stream from beginning
 *
 * @since 2.0.0
 */
public class ReusableByteArrayInputStream extends ByteArrayInputStream
{

  private final int initialOffset;

  private final int initialLength;

  public ReusableByteArrayInputStream(byte[] buf, int offset, int length)
  {
    super(buf, offset, length);
    this.initialLength = Math.min(offset + length, buf.length);
    this.initialOffset = offset;
  }

  public ReusableByteArrayInputStream(byte[] buf)
  {
    super(buf);
    this.initialLength = buf.length;
    this.initialOffset = 0;
  }

  public void renew()
  {
    pos = initialOffset;
    count = initialLength;
    mark = 0;
  }

  public int getPos()
  {
    return pos;
  }

  public byte[] getBuf()
  {
    return buf;
  }

}
