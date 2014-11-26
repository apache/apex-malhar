/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.benchmark;

import com.datatorrent.contrib.hive.AbstractHiveHDFS;
import com.datatorrent.contrib.hive.HiveStore;
import java.util.Iterator;
import java.util.Map;
import javax.validation.constraints.NotNull;



/**
 * Hive Insert Map Operator which implements inserting data of type map into Hive tables having
 * column of map data type from files written in hdfs.
 * @param <T>
 */
public class HiveMapInsertOperator<T> extends AbstractHiveHDFS<Map<T,T>, HiveStore>
{
  @NotNull
  public String delimiter;

  public String getDelimiter()
  {
    return delimiter;
  }

  public void setDelimiter(String delimiter)
  {
    this.delimiter = delimiter;
  }

  public HiveMapInsertOperator(){
    this.store = new HiveStore();
  }

  @Override
  public String getHiveTuple(Map tuple)
  {
    Iterator<T> keyIter = tuple.keySet().iterator();
    StringBuilder writeToHive = new StringBuilder("");

    while(keyIter.hasNext()){
     T key = keyIter.next();
     T obj = (T)tuple.get(key);
     writeToHive.append(key).append(delimiter).append(obj).append("\n");
    }
    return writeToHive.toString();
  }

}
