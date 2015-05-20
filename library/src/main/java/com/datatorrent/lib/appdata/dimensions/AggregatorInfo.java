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
import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class AggregatorInfo implements Serializable
{
  private static final long serialVersionUID = 20154301642L;

  private transient boolean setup = false;
  private transient Map<Class<? extends DimensionsIncrementalAggregator>, String> classToStaticAggregatorName;
  private transient Map<String, List<String>> otfAggregatorToStaticAggregators;
  private transient Map<String, DimensionsIncrementalAggregator> staticAggregatorNameToStaticAggregator;
  private transient Map<Integer, DimensionsIncrementalAggregator> staticAggregatorIDToAggregator;

  private Map<String, DimensionsIncrementalAggregator> nameToIncrementalAggregator;
  private Map<String, DimensionsOTFAggregator> nameToOTFAggregator;
  private Map<String, Integer> incrementalAggregatorNameToID;

  private static final Map<String, Integer> autoGenIds(Map<String, DimensionsIncrementalAggregator> nameToAggregator)
  {
    Map<String, Integer> staticAggregatorNameToID = Maps.newHashMap();

    for(Map.Entry<String, DimensionsIncrementalAggregator> entry: nameToAggregator.entrySet()) {
      staticAggregatorNameToID.put(entry.getKey(), stringHash(entry.getValue().getClass().getName()));
    }

    return staticAggregatorNameToID;
  }

  /**
   * <b>Note:</b> Do not change this function it will cause corruption for users updating existing data stores.
   * @return The hash of the given string
   */
  private static int stringHash(String string)
  {
    int hash = 5381;

    for(int index = 0;
        index < string.length();
        index++) {
      int character = (int) string.charAt(index);
      hash = hash * 33 + character;
    }

    return hash;
  }

  private AggregatorInfo()
  {
    //for kryo
  }

  public AggregatorInfo(Map<String, DimensionsIncrementalAggregator> nameToIncrementalAggregator,
                        Map<String, DimensionsOTFAggregator> nameToOTFAggregator)
  {
    this(nameToIncrementalAggregator,
         nameToOTFAggregator,
         autoGenIds(nameToIncrementalAggregator));
  }

  public AggregatorInfo(Map<String, DimensionsIncrementalAggregator> nameToIncrementalAggregator,
                        Map<String, DimensionsOTFAggregator> nameToOTFAggregator,
                        Map<String, Integer> incrementalAggregatorNameToID)
  {
    setNameToIncrementalAggregator(nameToIncrementalAggregator);
    setNameToOTFAggregator(nameToOTFAggregator);

    setIncrementalAggregatorNameToID(incrementalAggregatorNameToID);

    validate();
  }

  private void validate()
  {
    for(Map.Entry<String, DimensionsIncrementalAggregator> entry: nameToIncrementalAggregator.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }

    for(Map.Entry<String, DimensionsOTFAggregator> entry: nameToOTFAggregator.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Prec
    }

    for(Map.Entry<String, DimensionsAggregator> entry: nameToAggregator.entrySet()) {
      String aggregatorName = entry.getKey();
      DimensionsAggregator aggregator = entry.getValue();

      if(aggregator instanceof DimensionsOTFAggregator) {
        if(incrementalAggregatorNameToID.get(aggregatorName) != null) {
          throw new IllegalArgumentException("There should not be an id entry for an aggregator of type " +
                                             aggregator.getClass() +
                                             " and id " +
                                             incrementalAggregatorNameToID.get(aggregatorName));
        }
      }
      else if(aggregator instanceof DimensionsIncrementalAggregator) {
        Preconditions.checkArgument(incrementalAggregatorNameToID.get(aggregatorName) != null);
      }
      else {
        throw new IllegalArgumentException("Unsupported aggregator type " +
                                           aggregator.getClass());
      }
    }
  }

  public void setup()
  {
    if(setup) {
      return;
    }

    setup = true;

    staticAggregatorNameToStaticAggregator = Maps.newHashMap();
    nameToOTFAggregator = Maps.newHashMap();

    for(Map.Entry<String, DimensionsAggregator> entry: nameToAggregator.entrySet()) {
      String name = entry.getKey();
      DimensionsAggregator aggregator = entry.getValue();

      if(aggregator instanceof DimensionsIncrementalAggregator) {
        staticAggregatorNameToStaticAggregator.put(name, DimensionsIncrementalAggregator.class.cast(aggregator));
      }
      else if(aggregator instanceof DimensionsOTFAggregator) {
        nameToOTFAggregator.put(name, DimensionsOTFAggregator.class.cast(aggregator));
      }
      else {
        throw new UnsupportedOperationException("The class " + aggregator.getClass() + " is not supported");
      }
    }

    classToStaticAggregatorName = Maps.newHashMap();

    for(Map.Entry<String, DimensionsIncrementalAggregator> entry: staticAggregatorNameToStaticAggregator.entrySet()) {
      classToStaticAggregatorName.put(entry.getValue().getClass(), entry.getKey());
    }

    staticAggregatorIDToAggregator = Maps.newHashMap();

    for(Map.Entry<String, Integer> entry: incrementalAggregatorNameToID.entrySet()) {
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

      for(Class<? extends DimensionsIncrementalAggregator> clazz: dotfAggregator.getChildAggregators()) {
        staticAggregators.add(classToStaticAggregatorName.get(clazz));
      }

      //TODO make unmodifiable
      //otfAggregatorToStaticAggregators.put(name, Collections.unmodifiableList(staticAggregators));
      otfAggregatorToStaticAggregators.put(name, staticAggregators);
    }

    //TODO make this map unmodifiable
    //otfAggregatorToStaticAggregators = Collections.unmodifiableMap(otfAggregatorToStaticAggregators);
  }

  private void setNameToIncrementalAggregator(Map<String, DimensionsIncrementalAggregator> nameToIncrementalAggregator)
  {
    this.nameToIncrementalAggregator = Maps.newHashMap(Preconditions.checkNotNull(nameToIncrementalAggregator));
  }

  private void setNameToOTFAggregator(Map<String, DimensionsOTFAggregator> nameToAggregator)
  {
    this.nameToOTFAggregator = Maps.newHashMap(Preconditions.checkNotNull(nameToOTFAggregator));
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

  public Map<Class<? extends DimensionsIncrementalAggregator>, String> getClassToStaticAggregatorName()
  {
    return classToStaticAggregatorName;
  }

  public Map<Integer, DimensionsIncrementalAggregator> getStaticAggregatorIDToAggregator()
  {
    return staticAggregatorIDToAggregator;
  }

  public Map<String, DimensionsIncrementalAggregator> getStaticAggregatorNameToStaticAggregator()
  {
    return this.staticAggregatorNameToStaticAggregator;
  }

  private void setIncrementalAggregatorNameToID(Map<String, Integer> incrementalAggregatorNameToID)
  {
    Preconditions.checkNotNull(incrementalAggregatorNameToID);

    for(Map.Entry<String, Integer> entry: incrementalAggregatorNameToID.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }

    this.incrementalAggregatorNameToID = Maps.newHashMap(incrementalAggregatorNameToID);
    //TODO this map should be made unmodifiable
    //this.incrementalAggregatorNameToID = Collections.unmodifiableMap(incrementalAggregatorNameToID);
  }

  public Map<String, Integer> getStaticAggregatorNameToID()
  {
    return incrementalAggregatorNameToID;
  }

  public Map<String, DimensionsOTFAggregator> getNameToOTFAggregators()
  {
    return nameToOTFAggregator;
  }

  public Map<String, List<String>> getOTFAggregatorToStaticAggregators()
  {
    return otfAggregatorToStaticAggregators;
  }

  private static final Logger logger = LoggerFactory.getLogger(AggregatorInfo.class);
}
