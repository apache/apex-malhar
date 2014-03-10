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
package com.datatorrent.demos.mroperator;

import org.apache.hadoop.fs.Path;

import com.datatorrent.lib.io.fs.AbstractHdfsOutputOperator;
import com.datatorrent.lib.util.KeyHashValPair;

/**
 * Adapter for writing KeyHashValPair objects to HDFS
 * <p>
 * Serializes tuples into a HDFS file.<br/>
 * </p>
 * 
 * 
 */
public class HdfsKeyValOutputOperator<K,V> extends AbstractHdfsOutputOperator<KeyHashValPair<K,V>>
{
  
  private Path path;

  @Override
  public Path nextFilePath()
  {
    if(path == null){
      path = new Path(getFilePathPattern());
    }
    return path;
  }

  @Override
  public byte[] getBytesForTuple(KeyHashValPair<K,V> t)
  {
    return t.toString().getBytes();
  }

}
