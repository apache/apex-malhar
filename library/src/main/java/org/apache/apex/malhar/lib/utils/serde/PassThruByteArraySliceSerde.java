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
package org.apache.apex.malhar.lib.utils.serde;

import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.netlet.util.Slice;

/**
 * This is a simple {@link Serde} which serializes and deserializes byte arrays to {@link Slice}s. A byte array is
 * serialized by simply wrapping it in a {@link Slice} object and deserialized by simply reading the byte array
 * out of the {@link Slice} object.
 *
 * <b>Note:</b> The deserialized method doesn't use the offset argument in this implementation.
 *
 * @since 3.5.0
 */
public class PassThruByteArraySliceSerde implements Serde<byte[], Slice>
{
  @Override
  public Slice serialize(byte[] object)
  {
    return new Slice(object);
  }

  @Override
  public byte[] deserialize(Slice object, MutableInt offset)
  {
    offset.add(object.length);

    if (object.offset == 0) {
      return object.buffer;
    }

    byte[] bytes = new byte[object.length];
    System.arraycopy(object.buffer, object.offset, bytes, 0, object.length);
    return bytes;
  }

  @Override
  public byte[] deserialize(Slice object)
  {
    return deserialize(object, new MutableInt(0));
  }
}
