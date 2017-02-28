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
package org.apache.apex.malhar.lib.window.accumulation;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * Left Outer join Accumulation for Pojo Streams.
 *
 */
@InterfaceStability.Evolving
public class PojoLeftOuterJoin<InputT1, InputT2>
    extends AbstractPojoJoin<InputT1, InputT2>
{
  public PojoLeftOuterJoin()
  {
   super();
  }

  public PojoLeftOuterJoin(int num, Class<?> outClass, String... keys)
  {
    super(outClass,keys);

  }

  @Override
  public void addNonMatchingResult(List result, Map requiredMap, Set nullFields)
  {
    for (Object field : nullFields) {
      if (!keySetStream2.contains(field)) {
        requiredMap.put(field.toString(), null);
      }
    }
    result.add(requiredMap);
  }

  @Override
  public int getLeftStreamIndex()
  {
    return 0;
  }

}
