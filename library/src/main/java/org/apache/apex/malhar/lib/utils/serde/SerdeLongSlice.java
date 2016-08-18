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

import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.netlet.util.Slice;

public class SerdeLongSlice implements Serde<Long, Slice>
{
  @Override
  public Slice serialize(Long object)
  {
    return new Slice(GPOUtils.serializeLong(object));
  }

  @Override
  public Long deserialize(Slice slice, MutableInt offset)
  {
    return GPOUtils.deserializeLong(slice.buffer, offset);
  }

  @Override
  public Long deserialize(Slice object)
  {
    return deserialize(object, new MutableInt(0));
  }
}

