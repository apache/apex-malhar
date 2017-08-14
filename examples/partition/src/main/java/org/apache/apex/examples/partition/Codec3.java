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

package org.apache.apex.examples.partition;

import org.apache.apex.malhar.lib.codec.KryoSerializableStreamCodec;

/**
 * @since 3.7.0
 */
public class Codec3 extends KryoSerializableStreamCodec<Integer>
{
  @Override
  public int getPartition(Integer tuple)
  {
    final int v = tuple;
    return (1 == (v & 1)) ? 0      // odd
           : (0 == (v & 3)) ? 1      // divisible by 4
           : 2;                      // divisible by 2 but not 4
  }
}
