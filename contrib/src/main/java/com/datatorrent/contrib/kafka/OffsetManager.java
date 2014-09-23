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
package com.datatorrent.contrib.kafka;

import java.util.Map;

/**
 * An offset manager interface used by  {@link AbstractPartitionableKafkaInputOperator} to notify any offset change from any of the operator instances
 * <br>
 * It's also used to load the initial offset from user defined way. Ex. you could write offset to hdfs and load it back
 *
 */
public interface OffsetManager
{

  /**
   * A user defined way to load initial offset
   */
  public void loadInitialOffset();

  /**
   * Get all the offset map for all kafka partitions
   * @return Offset Map, kafka partition id as key, offset for each partition as value
   */
  public Map<Integer, Long> getAllOffset();


  /**
   * @param offsetsOfPartitions offsets for specified partitions
   * 
   * It will be called if offset of specified partitions are changed
   */
  public void updateOffsets(Map<Integer, Long> offsetsOfPartitions);

}
