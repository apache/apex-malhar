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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class AggregatorInfo
{
  private static final Logger logger = LoggerFactory.getLogger(AggregatorInfo.class);

  private Map<Class<? extends DimensionsStaticAggregator>, String> classToStaticAggregatorName;
  private Map<String, DimensionsOTFAggregator> nameToOTFAggregator;
  private Map<String, List<String>> otfAggregatorToStaticAggregators;
  private Map<String, DimensionsStaticAggregator> staticAggregatorNameToStaticAggregator;
  private Map<String, Integer> staticAggregatorNameToID;
  private Map<Integer, DimensionsStaticAggregator> staticAggregatorIDToAggregator;

  public AggregatorInfo()
  {
    //for kryo
  }

  public AggregatorInfo(Map<String, DimensionsAggregator> nameToAggregator,
                        Map<String, Integer> staticAggregatorNameToID)
  {
    setStaticAggregatorNameToID(staticAggregatorNameToID);
    initialize(nameToAggregator);
  }

  private void initialize(Map<String, DimensionsAggregator> nameToAggregator)
  {
    staticAggregatorNameToStaticAggregator = Maps.newHashMap();
    nameToOTFAggregator = Maps.newHashMap();

    for(Map.Entry<String, DimensionsAggregator> entry: nameToAggregator.entrySet()) {
      String name = entry.getKey();
      DimensionsAggregator aggregator = entry.getValue();

      if(aggregator instanceof DimensionsStaticAggregator) {
        staticAggregatorNameToStaticAggregator.put(name, DimensionsStaticAggregator.class.cast(aggregator));
      }
      else if(aggregator instanceof DimensionsOTFAggregator) {
        nameToOTFAggregator.put(name, DimensionsOTFAggregator.class.cast(aggregator));
      }
      else {
        throw new UnsupportedOperationException("The class " + aggregator.getClass() + " is not supported");
      }
    }

    classToStaticAggregatorName = Maps.newHashMap();

    for(Map.Entry<String, DimensionsStaticAggregator> entry: staticAggregatorNameToStaticAggregator.entrySet()) {
      classToStaticAggregatorName.put(entry.getValue().getClass(), entry.getKey());
    }

    staticAggregatorIDToAggregator = Maps.newHashMap();

    for(Map.Entry<String, Integer> entry: staticAggregatorNameToID.entrySet()) {
      String aggregatorName = entry.getKey();
      int aggregatorID = entry.getValue();
      staticAggregatorIDToAggregator.put(aggregatorID,
                                         staticAggregatorNameToStaticAggregator.get(aggregatorName));
    }

    otfAggregatorToStaticAggregators = Maps.newHashMap();

    for(Map.Entry<String, DimensionsOTFAggregator> entry: nameToOTFAggregator.entrySet()) {
      String name = entry.getKey();
      List<String> staticAggregators = Lists.newArrayList();

      DimensionsOTFAggregator dotfAggregator = nameToOTFAggregator.get(name);

      for(Class<? extends DimensionsStaticAggregator> clazz: dotfAggregator.getChildAggregators()) {
        staticAggregators.add(classToStaticAggregatorName.get(clazz));
      }

      //TODO make unmodifiable
      //otfAggregatorToStaticAggregators.put(name, Collections.unmodifiableList(staticAggregators));
      otfAggregatorToStaticAggregators.put(name, staticAggregators);
    }

    //TODO make this map unmodifiable
    //otfAggregatorToStaticAggregators = Collections.unmodifiableMap(otfAggregatorToStaticAggregators);
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

  public Map<Class<? extends DimensionsStaticAggregator>, String> getClassToStaticAggregatorName()
  {
    return classToStaticAggregatorName;
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
    //TODO this map should be made unmodifiable
    //this.staticAggregatorNameToStaticAggregator = Collections.unmodifiableMap(staticAggregatorNameToStaticAggregator);
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
    //TODO this map should be made unmodifiable
    //this.staticAggregatorNameToID = Collections.unmodifiableMap(staticAggregatorNameToID);
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
    //TODO this map should be made unmodifiable
    //this.nameToOTFAggregator = Collections.unmodifiableMap(nameToOTFAggregator);
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
