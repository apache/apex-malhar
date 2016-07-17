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

import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.netlet.util.Slice;

/**
 * An implementation of {@link Serde} which serializes and deserializes {@link String}s.
 */
@InterfaceStability.Evolving
public class SerdeStringSlice implements Serde<String, Slice>
{
  @Override
  public Slice serialize(String object)
  {
    return new Slice(GPOUtils.serializeString(object));
  }

  @Override
  public String deserialize(Slice object, MutableInt offset)
  {
    offset.add(object.offset);
    String string = GPOUtils.deserializeString(object.buffer, offset);
    offset.subtract(object.offset);
    return string;
  }

  @Override
  public String deserialize(Slice object)
  {
    return deserialize(object, new MutableInt(0));
  }
}
