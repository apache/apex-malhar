/**
 * 
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.datatorrent.contrib.kinesis;

import com.datatorrent.common.util.Pair;

/**
 * 
 * @displayName Kinesis Put Output
 * @category Database
 * @tags Kinesis put, output operator, ByteArray
 */
public class KinesisByteArrayOutputOperator extends AbstractKinesisOutputOperator<byte[], Pair<String, byte[]>>
{

  @Override
  protected byte[] getRecord(byte[] value)
  {
    return value;
  }

  @Override
  protected Pair<String, byte[]> tupleToKeyValue(Pair<String, byte[]> tuple)
  {
    return tuple;
  }

}
