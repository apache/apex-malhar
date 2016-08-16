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
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.netlet.util.Slice;

/**
 * This is a {@link Serde} implementation which simply allows an input slice to pass through. No serialization or
 * deserialization transformation is performed on the input {@link Slice}s.
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public class PassThruSliceSerde implements Serde<Slice>
{
  @Override
  public void serialize(Slice slice, SerializationBuffer buffer)
  {
    buffer.write(slice.buffer, slice.offset, slice.length);
  }

  @Override
  public Slice deserialize(byte[] buffer, MutableInt offset, int length)
  {
    return new Slice(buffer, offset.intValue(), length);
  }
}
