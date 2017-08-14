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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.util.PojoUtils;

import com.google.common.collect.Multimap;

/**
 * Inner join Accumulation for Pojo Streams.
 *
 * @since 3.6.0
 */
public class PojoInnerJoin<InputT1, InputT2>
    extends AbstractPojoJoin<InputT1, InputT2>
{
  public PojoInnerJoin()
  {
   super();
  }

  @Deprecated
  public PojoInnerJoin(int num, Class<?> outClass, String... keys)
  {
    this.outClass = outClass;
    if (keys.length % 2 != 0) {
      throw new IllegalArgumentException("Wrong number of keys.");
    }
    this.leftKeys = new String[keys.length / 2];
    this.rightKeys = new String[keys.length / 2];
    for (int i = 0,j = 0; i < keys.length; i = i + 2, j++) {
      this.leftKeys[j] = keys[i];
      this.rightKeys[j] = keys[i + 1];
    }
  }

  public PojoInnerJoin(Class<?> outClass, String[] leftKeys, String[] rightKeys)
  {
    super(outClass,leftKeys,rightKeys);
  }

  public PojoInnerJoin(Class<?> outClass, String[] leftKeys, String[] rightKeys, Map<String, KeyValPair<STREAM, String>> outputToInputMap)
  {
    super(outClass,leftKeys,rightKeys, outputToInputMap);
  }

  @Override
  public void addNonMatchingResult(Collection<Object> left, Map<String,PojoUtils.Getter> leftGettersStream, List<Object> result)
  {
    return;
  }

  @Override
  public void addNonMatchingRightStream(Multimap<List<Object>, Object> rightStream,
      Map<String,PojoUtils.Getter> rightGettersStream, List<Object> result)
  {
    return;
  }

  @Override
  public int getLeftStreamIndex()
  {
    return 0;
  }
}
