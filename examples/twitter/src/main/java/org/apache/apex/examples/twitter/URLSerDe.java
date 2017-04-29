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
package org.apache.apex.examples.twitter;

import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.netlet.util.Slice;

/**
 * <p>URLSerDe class.</p>
 *
 * @since 0.3.2
 */
public class URLSerDe implements StreamCodec<byte[]>
{
  /**
   * Covert the bytes into object useful for downstream node.
   *
   * @param fragment
   * @return WindowedURLHolder object which represents the bytes.
   */
  @Override
  public byte[] fromByteArray(Slice fragment)
  {
    if (fragment == null || fragment.buffer == null) {
      return null;
    } else if (fragment.offset == 0 && fragment.length == fragment.buffer.length) {
      return fragment.buffer;
    } else {
      byte[] buffer = new byte[fragment.buffer.length];
      System.arraycopy(fragment.buffer, fragment.offset, buffer, 0, fragment.length);
      return buffer;
    }
  }

  /**
   * Cast the input object to byte[].
   *
   * @param object - byte array representing the bytes of the string
   * @return the same object as input
   */
  @Override
  public Slice toByteArray(byte[] object)
  {
    return new Slice(object, 0, object.length);
  }

  @Override
  public int getPartition(byte[] object)
  {
    ByteBuffer bb = ByteBuffer.wrap(object);
    return bb.hashCode();
  }

  private static final Logger logger = LoggerFactory.getLogger(URLSerDe.class);
}
