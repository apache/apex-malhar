/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
 */
package com.datatorrent.contrib.kinesis;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * An shard manager interface used by  AbstractPartitionableKinesisInputOperator to define the customized initial positions and periodically update the current shard positions of all the operators
 * Ex. you could write shardManager to hdfs and load it back when restart the application
 *
 */
public class ShardManager
{

  protected final transient Map<String, String> shardPos = Collections.synchronizedMap(new HashMap<String, String>());
  /**
   * Load initial positions for all kinesis Shards
   * The method is called at the first attempt of creating shards and the return value is used as initial positions for simple consumer
   *
   * @return Map of Kinesis shard id as key and sequence id as value
   */
  public Map<String, String> loadInitialShardPositions()
  {
    return shardPos;
  }

  /**
   * @param shardPositions positions for specified shards, it is reported by individual operator instances
   * The method is called every AbstractPartitionableKinesisInputOperator.getRepartitionCheckInterval() to update the current positions
   */
  public void updatePositions(Map<String, String> shardPositions)
  {
    shardPos.putAll(shardPositions);
  }
}