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

package com.datatorrent.lib.dimensions;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class AggregatorRegistry implements Serializable
{
  private static final long serialVersionUID = 20154301642L;

  private transient boolean setup = false;
  private transient Map<Class<? extends IncrementalAggregator>, String> classToIncrementalAggregatorName;
  private transient Map<String, List<String>> otfAggregatorToIncrementalAggregators;
  private transient Map<Integer, IncrementalAggregator> incrementalAggregatorIDToAggregator;

  private Map<String, IncrementalAggregator> nameToIncrementalAggregator;
  private Map<String, OTFAggregator> nameToOTFAggregator;
  private Map<String, Integer> incrementalAggregatorNameToID;

  private static Map<String, Integer> autoGenIds(Map<String, IncrementalAggregator> nameToAggregator)
  {
    Map<String, Integer> staticAggregatorNameToID = Maps.newHashMap();

    for(Map.Entry<String, IncrementalAggregator> entry: nameToAggregator.entrySet()) {
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

  private AggregatorRegistry()
  {
    //for kryo
  }

  public AggregatorRegistry(Map<String, IncrementalAggregator> nameToIncrementalAggregator,
                            Map<String, OTFAggregator> nameToOTFAggregator)
  {
    this(nameToIncrementalAggregator,
         nameToOTFAggregator,
         autoGenIds(nameToIncrementalAggregator));
  }

  public AggregatorRegistry(Map<String, IncrementalAggregator> nameToIncrementalAggregator,
                        Map<String, OTFAggregator> nameToOTFAggregator,
                        Map<String, Integer> incrementalAggregatorNameToID)
  {
    setNameToIncrementalAggregator(nameToIncrementalAggregator);
    setNameToOTFAggregator(nameToOTFAggregator);

    setIncrementalAggregatorNameToID(incrementalAggregatorNameToID);

    validate();
  }

  private void validate()
  {
    for(Map.Entry<String, IncrementalAggregator> entry: nameToIncrementalAggregator.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }

    for(Map.Entry<String, OTFAggregator> entry: nameToOTFAggregator.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }

    for(Map.Entry<String, Integer> entry: incrementalAggregatorNameToID.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }
  }

  public void setup()
  {
    if(setup) {
      return;
    }

    setup = true;

    classToIncrementalAggregatorName = Maps.newHashMap();

    for(Map.Entry<String, IncrementalAggregator> entry: nameToIncrementalAggregator.entrySet()) {
      classToIncrementalAggregatorName.put(entry.getValue().getClass(), entry.getKey());
    }

    incrementalAggregatorIDToAggregator = Maps.newHashMap();

    for(Map.Entry<String, Integer> entry: incrementalAggregatorNameToID.entrySet()) {
      String aggregatorName = entry.getKey();
      int aggregatorID = entry.getValue();
      incrementalAggregatorIDToAggregator.put(aggregatorID,
                                              nameToIncrementalAggregator.get(aggregatorName));
    }

    otfAggregatorToIncrementalAggregators = Maps.newHashMap();

    for(Map.Entry<String, OTFAggregator> entry: nameToOTFAggregator.entrySet()) {
      String name = entry.getKey();
      List<String> staticAggregators = Lists.newArrayList();

      OTFAggregator dotfAggregator = nameToOTFAggregator.get(name);

      for(Class<? extends IncrementalAggregator> clazz: dotfAggregator.getChildAggregators()) {
        staticAggregators.add(classToIncrementalAggregatorName.get(clazz));
      }

      //TODO make unmodifiable
      //otfAggregatorToStaticAggregators.put(name, Collections.unmodifiableList(staticAggregators));
      otfAggregatorToIncrementalAggregators.put(name, staticAggregators);
    }

    //TODO make this map unmodifiable
    //otfAggregatorToStaticAggregators = Collections.unmodifiableMap(otfAggregatorToStaticAggregators);
  }

  private void setNameToIncrementalAggregator(Map<String, IncrementalAggregator> nameToIncrementalAggregator)
  {
    this.nameToIncrementalAggregator = Maps.newHashMap(Preconditions.checkNotNull(nameToIncrementalAggregator));
  }

  private void setNameToOTFAggregator(Map<String, OTFAggregator> nameToOTFAggregator)
  {
    this.nameToOTFAggregator = Maps.newHashMap(Preconditions.checkNotNull(nameToOTFAggregator));
  }

  public boolean isAggregator(String aggregatorName)
  {
    return classToIncrementalAggregatorName.values().contains(aggregatorName) ||
           nameToOTFAggregator.containsKey(aggregatorName);
  }

  public boolean isStaticAggregator(String aggregatorName)
  {
    return classToIncrementalAggregatorName.values().contains(aggregatorName);
  }

  public Map<Class<? extends IncrementalAggregator>, String> getClassToIncrementalAggregatorName()
  {
    return classToIncrementalAggregatorName;
  }

  public Map<Integer, IncrementalAggregator> getIncrementalAggregatorIDToAggregator()
  {
    return incrementalAggregatorIDToAggregator;
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

  public Map<String, Integer> getIncrementalAggregatorNameToID()
  {
    return incrementalAggregatorNameToID;
  }

  public Map<String, OTFAggregator> getNameToOTFAggregators()
  {
    return nameToOTFAggregator;
  }

  public Map<String, List<String>> getOTFAggregatorToStaticAggregators()
  {
    return otfAggregatorToIncrementalAggregators;
  }

  /**
   * @return the nameToIncrementalAggregator
   */
  public Map<String, IncrementalAggregator> getNameToIncrementalAggregator()
  {
    return nameToIncrementalAggregator;
  }

  private static final Logger logger = LoggerFactory.getLogger(AggregatorRegistry.class);
}
