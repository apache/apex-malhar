/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions.aggregator;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * <p>
 * This registry is used by generic dimensions computation operators and dimension stores in order to support
 * plugging different
 * aggregators into the operator. Subclasses of
 * {@link com.datatorrent.lib.dimensions.AbstractDimensionsComputationFlexibleSingleSchema} use this registry
 * to support pluggable aggregators when doing dimensions computation, and Subclasses of
 * AppDataSingleSchemaDimensionStoreHDHT use this class as well.
 * </p>
 * <p>
 * The primary purpose of an {@link AggregatorRegistry} is to provide a mapping from aggregator names to aggregators,
 * and to provide mappings from aggregator IDs to aggregators. These mappings are necessary in order to correctly
 * process schemas, App Data queries, and store aggregated data.
 * </p>
 *
 * @since 3.1.0
 */
public class AggregatorRegistry implements Serializable
{
  private static final long serialVersionUID = 20154301642L;

  /**
   * This is a map from {@link IncrementalAggregator} names to {@link IncrementalAggregator}s used by the
   * default {@link AggregatorRegistry}.
   */
  private static final transient Map<String, IncrementalAggregator> DEFAULT_NAME_TO_INCREMENTAL_AGGREGATOR;
  /**
   * This is a map from {@link OTFAggregator} names to {@link OTFAggregator}s used by the default
   * {@link AggregatorRegistry}.
   */
  private static final transient Map<String, OTFAggregator> DEFAULT_NAME_TO_OTF_AGGREGATOR;

  //Build the default maps
  static {
    DEFAULT_NAME_TO_INCREMENTAL_AGGREGATOR = Maps.newHashMap(AggregatorIncrementalType.NAME_TO_AGGREGATOR);
    DEFAULT_NAME_TO_OTF_AGGREGATOR = Maps.newHashMap(AggregatorOTFType.NAME_TO_AGGREGATOR);
  }

  /**
   * This is a default aggregator registry that can be used in operators.
   */
  public static final AggregatorRegistry DEFAULT_AGGREGATOR_REGISTRY = new AggregatorRegistry(
      DEFAULT_NAME_TO_INCREMENTAL_AGGREGATOR, DEFAULT_NAME_TO_OTF_AGGREGATOR,
      AggregatorIncrementalType.NAME_TO_ORDINAL);

  /**
   * This is a flag indicating whether or not this {@link AggregatorRegistry} has been setup before or not.
   */
  private transient boolean setup = false;
  /**
   * This is a map from the class of an {@link IncrementalAggregator} to the name of that
   * {@link IncrementalAggregator}.
   */
  private transient Map<Class<? extends IncrementalAggregator>, String> classToIncrementalAggregatorName;
  /**
   * This is a map from the name of an {@link OTFAggregator} to the list of the names of all
   * {@link IncrementalAggregator} that are child aggregators of that {@link OTFAggregator}.
   */
  private transient Map<String, List<String>> otfAggregatorToIncrementalAggregators;
  /**
   * This is a map from the aggregator ID of an
   * {@link IncrementalAggregator} to the corresponding {@link IncrementalAggregator}.
   */
  private transient Map<Integer, IncrementalAggregator> incrementalAggregatorIDToAggregator;
  /**
   * This is a map from the name assigned to an {@link IncrementalAggregator} to the {@link IncrementalAggregator}.
   */
  private Map<String, IncrementalAggregator> nameToIncrementalAggregator;
  /**
   * This is a map from the name assigned to an {@link OTFAggregator} to the {@link OTFAggregator}.
   */
  private Map<String, OTFAggregator> nameToOTFAggregator;
  /**
   * This is a map from the name of an {@link IncrementalAggregator} to the ID of that {@link IncrementalAggregator}.
   */
  private Map<String, Integer> incrementalAggregatorNameToID;

  /**
   * This is a helper method used to autogenerate the IDs for each {@link IncrementalAggregator}
   *
   * @param nameToAggregator A mapping from the name of an {@link IncrementalAggregator} to the
   *                         {@link IncrementalAggregator}.
   * @return A mapping from the name of an {@link IncrementalAggregator} to the ID assigned to that
   * {@link IncrementalAggregator}.
   */
  private static Map<String, Integer> autoGenIds(Map<String, IncrementalAggregator> nameToAggregator)
  {
    Map<String, Integer> staticAggregatorNameToID = Maps.newHashMap();

    for (Map.Entry<String, IncrementalAggregator> entry : nameToAggregator.entrySet()) {
      staticAggregatorNameToID.put(entry.getKey(), stringHash(entry.getValue().getClass().getName()));
    }

    return staticAggregatorNameToID;
  }

  /**
   * This is a helper method for computing the hash of the string. This is intended to be a static unchanging
   * method since the computed hash is used for aggregator IDs which are used for persistence.
   * <p>
   * <b>Note:</b> Do not change this function it will cause corruption for users updating existing data stores.
   * </p>
   *
   * @return The hash of the given string.
   */
  private static int stringHash(String string)
  {
    int hash = 5381;

    for (int index = 0;
        index < string.length();
        index++) {
      int character = (int)string.charAt(index);
      hash = hash * 33 + character;
    }

    return hash;
  }

  /**
   * This constructor is present for Kryo serialization
   */
  private AggregatorRegistry()
  {
    //for kryo
  }

  /**
   * <p>
   * This creates an {@link AggregatorRegistry} which assigns the given names to the given
   * {@link IncrementalAggregator}s and {@link OTFAggregator}s. This constructor also auto-generates
   * the IDs associated with each {@link IncrementalAggregator} by computing the hashcode of the
   * fully qualified class name of each {@link IncrementalAggregator}.
   * </p>
   * <p>
   * <b>Note:</b> IDs only need to be generated for {@link IncrementalAggregator}s since they are the
   * only type of stored aggregations. {@link OTFAggregator}s do not require an ID since they are not stored.
   * </p>
   *
   * @param nameToIncrementalAggregator This is a map from {@link String} to {@link IncrementalAggregator},
   *                                    where the string is the name of an
   *                                    {@link IncrementalAggregator} and the value is the {@link IncrementalAggregator}
   *                                    with that name.
   * @param nameToOTFAggregator         This is a map from {@link String} to {@link OTFAggregator}, where the string
   *                                    is the name of
   *                                    an {@link OTFAggregator} and the value is the {@link OTFAggregator} with that
   *                                    name.
   */
  public AggregatorRegistry(Map<String, IncrementalAggregator> nameToIncrementalAggregator,
      Map<String, OTFAggregator> nameToOTFAggregator)
  {
    this(nameToIncrementalAggregator,
        nameToOTFAggregator,
        autoGenIds(nameToIncrementalAggregator));
  }

  /**
   * <p>
   * This creates an {@link AggregatorRegistry} which assigns the given names to the given
   * {@link IncrementalAggregator}s and {@link OTFAggregator}s. This constructor assigns IDs to each
   * {@link IncrementalAggregator} by using the provided map from incremental aggregator names to IDs.
   * </p>
   * <p>
   * <b>Note:</b> IDs only need to be generated for {@link IncrementalAggregator}s since they are the
   * only type of stored aggregations. {@link OTFAggregator}s do not require an ID since they are not stored.
   * </p>
   *
   * @param nameToIncrementalAggregator   This is a map from {@link String} to {@link IncrementalAggregator},
   *                                      where the string is the name of an
   *                                      {@link IncrementalAggregator} and the value is the
   *                                      {@link IncrementalAggregator}
   *                                      with that name.
   * @param nameToOTFAggregator           This is a map from {@link String} to {@link OTFAggregator}, where the
   *                                      string is the name of
   *                                      an {@link OTFAggregator} and the value is the {@link OTFAggregator} with
   *                                      that name.
   * @param incrementalAggregatorNameToID This is a map from the name of an {@link IncrementalAggregator} to the ID
   *                                      for that
   *                                      {@link IncrementalAggregator}.
   */
  public AggregatorRegistry(Map<String, IncrementalAggregator> nameToIncrementalAggregator,
      Map<String, OTFAggregator> nameToOTFAggregator,
      Map<String, Integer> incrementalAggregatorNameToID)
  {
    setNameToIncrementalAggregator(nameToIncrementalAggregator);
    setNameToOTFAggregator(nameToOTFAggregator);

    setIncrementalAggregatorNameToID(incrementalAggregatorNameToID);

    validate();
  }

  /**
   * This is a helper method which is used to do validation on the maps provided to the constructor of this class.
   */
  private void validate()
  {
    for (Map.Entry<String, IncrementalAggregator> entry : nameToIncrementalAggregator.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }

    for (Map.Entry<String, OTFAggregator> entry : nameToOTFAggregator.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }

    for (Map.Entry<String, Integer> entry : incrementalAggregatorNameToID.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }
  }

  /**
   * This method is called to initialize various internal datastructures of the {@link AggregatorRegistry}.
   * This method should be called before the {@link AggregatorRegistry} is used.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void setup()
  {
    if (setup) {
      //If the AggregatorRegistry was already setup. Don't set it up again.
      return;
    }

    setup = true;

    classToIncrementalAggregatorName = Maps.newHashMap();

    for (Map.Entry<String, IncrementalAggregator> entry : nameToIncrementalAggregator.entrySet()) {
      classToIncrementalAggregatorName.put((Class)entry.getValue().getClass(), entry.getKey());
    }

    incrementalAggregatorIDToAggregator = Maps.newHashMap();

    for (Map.Entry<String, Integer> entry : incrementalAggregatorNameToID.entrySet()) {
      String aggregatorName = entry.getKey();
      int aggregatorID = entry.getValue();
      incrementalAggregatorIDToAggregator.put(aggregatorID,
          nameToIncrementalAggregator.get(aggregatorName));
    }

    otfAggregatorToIncrementalAggregators = Maps.newHashMap();

    for (Map.Entry<String, OTFAggregator> entry : nameToOTFAggregator.entrySet()) {
      String name = entry.getKey();
      List<String> staticAggregators = Lists.newArrayList();

      OTFAggregator dotfAggregator = nameToOTFAggregator.get(name);

      for (Class clazz : dotfAggregator.getChildAggregators()) {
        staticAggregators.add(classToIncrementalAggregatorName.get(clazz));
      }

      otfAggregatorToIncrementalAggregators.put(name, staticAggregators);
    }
  }

  /**
   * This is a helper method which sets and validated the given mapping from an {@link IncrementalAggregator}'s name
   * to an {@link IncrementalAggregator}.
   *
   * @param nameToIncrementalAggregator The mapping from an {@link IncrementalAggregator}'s name to an
   *                                    {@link IncrementalAggregator}.
   */
  private void setNameToIncrementalAggregator(Map<String, IncrementalAggregator> nameToIncrementalAggregator)
  {
    this.nameToIncrementalAggregator = Maps.newHashMap(Preconditions.checkNotNull(nameToIncrementalAggregator));
  }

  /**
   * This is a helper method which sets and validates the given mapping from an {@link OTFAggregator}'s name to
   * an {@link OTFAggregator}.
   *
   * @param nameToOTFAggregator The mapping from an {@link OTFAggregator}'s name to an {@link OTFAggregator}.
   */
  private void setNameToOTFAggregator(Map<String, OTFAggregator> nameToOTFAggregator)
  {
    this.nameToOTFAggregator = Maps.newHashMap(Preconditions.checkNotNull(nameToOTFAggregator));
  }

  /**
   * Checks if the given aggregatorName is the name of an {@link IncrementalAggregator} or {@link OTFAggregator}
   * registered to this registry.
   *
   * @param aggregatorName The aggregator name to check.
   * @return True if the given aggregator name is the name of an {@link IncrementalAggregator} registered to
   * this registry. False otherwise.
   */
  public boolean isAggregator(String aggregatorName)
  {
    return classToIncrementalAggregatorName.values().contains(aggregatorName) ||
        nameToOTFAggregator.containsKey(aggregatorName);
  }

  /**
   * Checks if the given aggregator name is the name of an {@link IncrementalAggregator} registered
   * to this registry.
   *
   * @param aggregatorName The aggregator name to check.
   * @return True if the given aggregator name is the name of an {@link IncrementalAggregator} registered
   * to this registry. False otherwise.
   */
  public boolean isIncrementalAggregator(String aggregatorName)
  {
    return classToIncrementalAggregatorName.values().contains(aggregatorName);
  }

  /**
   * Gets the mapping from an {@link IncrementalAggregator}'s class to the {@link IncrementalAggregator}.
   *
   * @return The mapping from an {@link IncrementalAggregator}'s class to the {@link IncrementalAggregator}.
   */
  public Map<Class<? extends IncrementalAggregator>, String> getClassToIncrementalAggregatorName()
  {
    return classToIncrementalAggregatorName;
  }

  /**
   * Gets the mapping from an {@link IncrementalAggregator}'s ID to the {@link IncrementalAggregator}.
   *
   * @return The mapping from an {@link IncrementalAggregator}'s ID to the {@link IncrementalAggregator}.
   */
  public Map<Integer, IncrementalAggregator> getIncrementalAggregatorIDToAggregator()
  {
    return incrementalAggregatorIDToAggregator;
  }

  /**
   * This a helper method which sets and validates the mapping from {@link IncrementalAggregator} name to
   * {@link IncrementalAggregator} ID.
   *
   * @param incrementalAggregatorNameToID The mapping from {@link IncrementalAggregator} name to
   *                                      {@link IncrementalAggregator} ID.
   */
  private void setIncrementalAggregatorNameToID(Map<String, Integer> incrementalAggregatorNameToID)
  {
    Preconditions.checkNotNull(incrementalAggregatorNameToID);

    for (Map.Entry<String, Integer> entry : incrementalAggregatorNameToID.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }

    this.incrementalAggregatorNameToID = Maps.newHashMap(incrementalAggregatorNameToID);
  }

  /**
   * This returns a map from the names of an {@link IncrementalAggregator}s to the corresponding ID of the
   * {@link IncrementalAggregator}.
   *
   * @return Returns a map from the names of an {@link IncrementalAggregator} to the corresponding ID of the
   * {@link IncrementalAggregator}.
   */
  public Map<String, Integer> getIncrementalAggregatorNameToID()
  {
    return incrementalAggregatorNameToID;
  }

  /**
   * Returns the name to {@link OTFAggregator} mapping, where the key is the name of the {@link OTFAggregator}.
   *
   * @return The name to {@link OTFAggregator} mapping.
   */
  public Map<String, OTFAggregator> getNameToOTFAggregators()
  {
    return nameToOTFAggregator;
  }

  /**
   * Returns the mapping from {@link OTFAggregator} names to a list of names of all the child aggregators of
   * that {@link OTFAggregator}.
   *
   * @return The mapping from {@link OTFAggregator} names to a list of names of all the child aggregators of
   * that {@link OTFAggregator}.
   */
  public Map<String, List<String>> getOTFAggregatorToIncrementalAggregators()
  {
    return otfAggregatorToIncrementalAggregators;
  }

  /**
   * Returns the name to {@link IncrementalAggregator} mapping, where the key is the name of the {@link OTFAggregator}.
   *
   * @return The name to {@link IncrementalAggregator} mapping.
   */
  public Map<String, IncrementalAggregator> getNameToIncrementalAggregator()
  {
    return nameToIncrementalAggregator;
  }

  private static final Logger lOG = LoggerFactory.getLogger(AggregatorRegistry.class);
}
