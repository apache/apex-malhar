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
package com.datatorrent.apps.telecom.cdr.simulator;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * A kafka partitioner class partition the records due to the type the tuples
 */
public class TypePartitioner implements Partitioner<String>
{

  public TypePartitioner (VerifiableProperties props) {
    
  }
  @Override
  public int partition(String key, int pNum)
  {
    if(key.equalsIgnoreCase("U") || key.equalsIgnoreCase("V"))
      return 0;
    else return 1;
  }

}
