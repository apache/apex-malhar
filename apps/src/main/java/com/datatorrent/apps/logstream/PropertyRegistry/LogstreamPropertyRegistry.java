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
package com.datatorrent.apps.logstream.PropertyRegistry;

import java.util.ArrayList;
import java.util.HashMap;

import org.slf4j.LoggerFactory;

/**
 * Logstream implementation of property registry
 * Properties are added to the registry during dag setup and are accessible to all operators.
 */
public class LogstreamPropertyRegistry implements PropertyRegistry<String>
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(LogstreamPropertyRegistry.class);
  private static PropertyRegistry<String> instance;
  private HashMap<Integer, ArrayList<String>> valueList = new HashMap<Integer, ArrayList<String>>();
  private ArrayList<String> nameList = new ArrayList<String>();
  private HashMap<String, Integer> indexMap = new HashMap<String, Integer>();

  @Override
  public synchronized int bind(String name, String value)
  {
    ArrayList<String> values = null;
    int nameIndex = nameList.indexOf(name);
    int valueIndex;

    if (nameIndex < 0) {
      nameList.add(name);
      nameIndex = nameList.indexOf(name);

      values = new ArrayList<String>();
      values.add(value);
      valueList.put(nameIndex, values);

      valueIndex = values.indexOf(value);
    }
    else {
      values = valueList.get(nameIndex);
      valueIndex = values.indexOf(value);
      if (valueIndex < 0) {
        values.add(value);
        valueIndex = values.indexOf(value);
      }
    }

    // first 16 characters represent the name, last 16 characters represent the value
    // there can be total of 2 ^ 16 names and 2 ^ 16 values for each name
    int index = nameIndex << 16 | valueIndex;

    indexMap.put(name + "_" + value, index);
    logger.debug("name = {} value = {} name index = {} value index = {} pair index = {}", name, value, nameIndex, valueIndex, index);

    return index;
  }

  @Override
  public String lookupValue(int index)
  {
    int nameIndex = index >> 16; // moves first 16 bits to last 16 bits
    int valueIndex = 0xffff & index; // clears out first 16 bits
    ArrayList<String> values = valueList.get(nameIndex);
    String value = values.get(valueIndex);

    return value;
  }

  @Override
  public String[] list(String name)
  {
    int nameIndex = nameList.indexOf(name);

    if (nameIndex < 0) {
      return new String[0];
    }

    ArrayList<String> values = valueList.get(nameIndex);

    return values.toArray(new String[values.size()]);
  }

  @Override
  public String lookupName(int index)
  {
    int nameIndex = index >> 16;
    String name = nameList.get(nameIndex);

    return name;
  }

  @Override
  public int getIndex(String name, String value)
  {
    Integer index = indexMap.get(name + "_" + value);
    if (index == null) {
      return -1;
    }
    else {
      return index;
    }
  }

  @Override
  public String toString()
  {
    return indexMap.toString();
  }

  public static PropertyRegistry<String> getInstance()
  {
    if (instance == null) {
      logger.error("registry instance is null");
    }
    return instance;
  }

  public static void setInstance(PropertyRegistry<String> registry)
  {
    instance = registry;
  }

}
