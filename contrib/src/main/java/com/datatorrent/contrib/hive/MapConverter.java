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
package com.datatorrent.contrib.hive;

import java.util.Iterator;
import java.util.Map;
import javax.validation.constraints.NotNull;

public class MapConverter implements Converter<Map<String,Object>>
{
  @NotNull
  public String delimiter=":";

  public MapConverter()
  {
  }

  public String getDelimiter()
  {
    return delimiter;
  }

  public void setDelimiter(String delimiter)
  {
    this.delimiter = delimiter;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public String getTuple(Map<String,Object> tuple)
  {
    Iterator<String> keyIter = tuple.keySet().iterator();
    StringBuilder writeToHive = new StringBuilder("");

    while (keyIter.hasNext()) {
      String key = keyIter.next();
      Object obj = tuple.get(key);
      writeToHive.append(key).append(delimiter).append(obj).append("\n");
    }
    return writeToHive.toString();
  }



}
