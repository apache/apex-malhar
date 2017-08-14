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
package org.apache.apex.malhar.lib.dedup;

import org.apache.apex.malhar.lib.codec.KryoSerializableStreamCodec;
import org.apache.apex.malhar.lib.util.PojoUtils;
import org.apache.apex.malhar.lib.util.PojoUtils.Getter;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import com.datatorrent.api.StreamCodec;

/**
 * A {@link StreamCodec} for {@link AbstractDeduper}.
 * This helps in partitioning the tuples depending on the key field in the tuple.
 * The {@link #getPartition(Object)} function returns the hash code of the key field
 *
 *
 * @since 3.5.0
 */
@Evolving
public class DeduperStreamCodec extends KryoSerializableStreamCodec<Object>
{
  private static final long serialVersionUID = -6904078808859412149L;

  private transient Getter<Object, Object> getter;
  private String keyExpression;

  public DeduperStreamCodec(String keyExpression)
  {
    this.keyExpression = keyExpression;
  }

  @Override
  public int getPartition(Object t)
  {
    if (getter == null) {
      getter = PojoUtils.createGetter(t.getClass(), keyExpression, Object.class);
    }
    return getter.get(t).hashCode();
  }
}
