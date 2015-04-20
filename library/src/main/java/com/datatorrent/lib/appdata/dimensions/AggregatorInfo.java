/*
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
 */

package com.datatorrent.lib.appdata.dimensions;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AggregatorInfo
{
  private Map<Class<? extends DimensionsStaticAggregator>, String> classToStaticAggregatorName;
  private Map<String, DimensionsOTFAggregator> nameToOTFAggregator;
  private Map<String, List<String>> otfAggregatorToStaticAggregators;
  private Map<String, DimensionsStaticAggregator> staticAggregatorNameToStaticAggregator;
  private Map<String, Integer> staticAggregatorNameToID;
  private Map<Integer, DimensionsStaticAggregator> staticAggregatorIDToAggregator;

  public AggregatorInfo(Map<Class<? extends DimensionsStaticAggregator>, String> classToStaticAggregatorName,
                        Map<Integer, DimensionsStaticAggregator> staticAggregatorIDToAggregator,
                        Map<String, DimensionsStaticAggregator> staticAggregatorNameToStaticAggregator,
                        Map<String, Integer> staticAggregatorNameToID,
                        Map<String, DimensionsOTFAggregator> nameToOTFAggregator)
  {
    setClassToStaticAggregatorName(classToStaticAggregatorName);
    setStaticAggregatorIDToAggregator(staticAggregatorIDToAggregator);
    setNameToOTFAggregators(nameToOTFAggregator);
    setStaticAggregatorNameToStaticAggregator(staticAggregatorNameToStaticAggregator);
    setStaticAggregatorNameToID(staticAggregatorNameToID);

    initialize();
  }

  private void initialize()
  {
    otfAggregatorToStaticAggregators = Maps.newHashMap();

    for(Map.Entry<String, DimensionsOTFAggregator> entry: nameToOTFAggregator.entrySet()) {
      String name = entry.getKey();
      List<String> staticAggregators = Lists.newArrayList();

      DimensionsOTFAggregator dotfAggregator = nameToOTFAggregator.get(name);

      for(Class<? extends DimensionsStaticAggregator> clazz: dotfAggregator.getChildAggregators()) {
        staticAggregators.add(classToStaticAggregatorName.get(clazz));
      }

      otfAggregatorToStaticAggregators.put(name, Collections.unmodifiableList(staticAggregators));
    }

    otfAggregatorToStaticAggregators = Collections.unmodifiableMap(otfAggregatorToStaticAggregators);
  }

  public boolean isAggregator(String aggregatorName)
  {
    return classToStaticAggregatorName.values().contains(aggregatorName) ||
           nameToOTFAggregator.containsKey(aggregatorName);
  }

  public boolean isStaticAggregator(String aggregatorName)
  {
    return classToStaticAggregatorName.values().contains(aggregatorName);
  }

  private void setClassToStaticAggregatorName(Map<Class<? extends DimensionsStaticAggregator>, String> staticAggregators)
  {
    Preconditions.checkNotNull(staticAggregators, "staticAggregators");

    for(Map.Entry<Class<? extends DimensionsStaticAggregator>, String> entry: staticAggregators.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }

    this.classToStaticAggregatorName = Maps.newHashMap(staticAggregators);
    this.classToStaticAggregatorName = Collections.unmodifiableMap(this.classToStaticAggregatorName);
  }

  public Map<Class<? extends DimensionsStaticAggregator>, String> getClassToStaticAggregatorName()
  {
    return classToStaticAggregatorName;
  }

  private void setStaticAggregatorIDToAggregator(Map<Integer, DimensionsStaticAggregator> staticAggregatorIDToAggregator)
  {
    Preconditions.checkNotNull(staticAggregatorIDToAggregator, "staticAggregatorIDToAggregator");

    for(Map.Entry<Integer, DimensionsStaticAggregator> entry: staticAggregatorIDToAggregator.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }

    this.staticAggregatorIDToAggregator = Maps.newHashMap(staticAggregatorIDToAggregator);
    this.staticAggregatorIDToAggregator = Collections.unmodifiableMap(staticAggregatorIDToAggregator);
  }

  public Map<Integer, DimensionsStaticAggregator> getStaticAggregatorIDToAggregator()
  {
    return staticAggregatorIDToAggregator;
  }

  private void setStaticAggregatorNameToStaticAggregator(Map<String, DimensionsStaticAggregator> staticAggregatorNameToStaticAggregator)
  {
    Preconditions.checkNotNull(staticAggregatorNameToStaticAggregator, "staticAggregatorNameToStaticAggregator");

    for(Map.Entry<String, DimensionsStaticAggregator> entry: staticAggregatorNameToStaticAggregator.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }

    this.staticAggregatorNameToStaticAggregator = Maps.newHashMap(staticAggregatorNameToStaticAggregator);
    this.staticAggregatorNameToStaticAggregator = Collections.unmodifiableMap(staticAggregatorNameToStaticAggregator);
  }

  public Map<String, DimensionsStaticAggregator> getStaticAggregatorNameToStaticAggregator()
  {
    return this.staticAggregatorNameToStaticAggregator;
  }

  private void setStaticAggregatorNameToID(Map<String, Integer> staticAggregatorNameToID)
  {
    Preconditions.checkNotNull(staticAggregatorNameToID);

    for(Map.Entry<String, Integer> entry: staticAggregatorNameToID.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }

    this.staticAggregatorNameToID = Maps.newHashMap(staticAggregatorNameToID);
    this.staticAggregatorNameToID = Collections.unmodifiableMap(staticAggregatorNameToID);
  }

  public Map<String, Integer> getStaticAggregatorNameToID()
  {
    return staticAggregatorNameToID;
  }

  private void setNameToOTFAggregators(Map<String, DimensionsOTFAggregator> nameToOTFAggregator)
  {
    Preconditions.checkNotNull(nameToOTFAggregator, "nameToOTFAggregator");

    for(Map.Entry<String, DimensionsOTFAggregator> entry: nameToOTFAggregator.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }

    this.nameToOTFAggregator = Maps.newHashMap(nameToOTFAggregator);
    this.nameToOTFAggregator = Collections.unmodifiableMap(nameToOTFAggregator);
  }

  public Map<String, DimensionsOTFAggregator> getNameToOTFAggregators()
  {
    return nameToOTFAggregator;
  }

  public Map<String, List<String>> getOTFAggregatorToStaticAggregators()
  {
    return otfAggregatorToStaticAggregators;
  }
}
