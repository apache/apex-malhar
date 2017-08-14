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
package org.apache.apex.malhar.hive;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class FSRollingMapTestImpl extends AbstractFSRollingOutputOperator<Map<String, Object>>
{
  @Override
  public ArrayList<String> getHivePartition(Map<String, Object> tuple)
  {
    ArrayList<String> hivePartitions = new ArrayList<String>();
    hivePartitions.add("2014-12-10");
    return (hivePartitions);
  }

  @Override
  protected byte[] getBytesForTuple(Map<String, Object> tuple)
  {
    Iterator<String> keyIter = tuple.keySet().iterator();
    StringBuilder writeToHive = new StringBuilder("");

    while (keyIter.hasNext()) {
      String key = keyIter.next();
      Object obj = tuple.get(key);
      writeToHive.append(key).append(":").append(obj).append("\n");
    }
    return writeToHive.toString().getBytes();
  }

}
