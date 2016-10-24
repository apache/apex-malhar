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

/**
 * AffixSerde provides serde for adding prefix or suffix
 *
 * @param <T>
 */
public class AffixSerde<T> implements Serde<T>
{
  protected Serde<T> serde;
  protected byte[] prefix;
  protected byte[] suffix;

  protected AffixSerde()
  {
    //kyro
  }

  public AffixSerde(byte[] prefix, Serde<T> serde, byte[] suffix)
  {
    this.prefix = prefix;
    this.suffix = suffix;
    this.serde = serde;
  }

  @Override
  public void serialize(T object, SerializationBuffer buffer)
  {
    if (buffer == null) {
      throw new IllegalArgumentException("Buffer should not null.");
    }
    if (prefix != null && prefix.length > 0) {
      buffer.write(prefix);
    }
    serde.serialize(object, buffer);
    if (suffix != null && suffix.length > 0) {
      buffer.write(suffix);
    }
  }

  @Override
  public T deserialize(byte[] buffer, MutableInt offset, int length)
  {
    if (prefix != null && prefix.length > 0) {
      offset.add(prefix.length);
    }
    return serde.deserialize(buffer, offset, length);
  }

}
