/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.sales.generic;

import com.google.common.collect.Lists;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.util.*;

/**
 * Provides a mechanism for selecting from a fixed set of objects at random.  Each object's
 * probability of being selected is directly proportional to corresponding weight.  This weight
 * can be set automatically to default value, or specified manually as objects are added to the
 * collection.  A move operation is supported, which allows for random adjustments to respective
 * weights of each object, and changing its probability of being selected.  These probability
 * re-distributions of object selection are bound with minWeight and maxWeight values.  Random
 * increments during move operation are selected using Gaussian distribution with moveDeviation
 * step size multiplier.
 *
 * @param <E> Any object to be returned in random weighted fashion
 *
 * @since 2.0.0
 */
class RandomWeightedMovableGenerator<E> {
  private NavigableMap<Double, E> map = new TreeMap<Double, E>();
  private final Random random;
  private double total = 0;
  // Amount of movement amplification
  private double moveDeviation = 1.0;
  // Minimum weight per
  private double minWeight = 0.01;
  private double maxWeight = 10.0;
  private double defaultWeight = 5.0;

  public RandomWeightedMovableGenerator() {
    this(new Random());
  }

  public RandomWeightedMovableGenerator(Random random) {
    this.random = random;
  }

  /**
   * Add new element to this collection with default weight.  All elements added with this method
   * will have same probability of being selected.
   * @param value value to store
   */
  public void add(E value) {
    total += defaultWeight;
    map.put(total, value);
  }

  /**
   * Add new element with specified weight.  Weight relative to other elements.
   * @param value value to store
   * @param weight weighted determines probability of selecting this element
   */
  public void add(E value, double weight) {
    if (weight <= 0) return;
    // Adjust min and max weights depending on weight provided
    if (weight > maxWeight) maxWeight = weight;
    if (weight < minWeight) minWeight = weight;
    total += weight;
    map.put(total, value);
  }

  public E next() {
    double value = random.nextDouble() * total;
    return map.ceilingEntry(value).getValue();
  }

  /**
   * Updates all the weights in the probability map by performing random
   * Gaussian movements on them.  Defaults to using local moveDeviation setting.
   */
  public void move() {
    move(getMoveDeviation());
  }

  /**
   * Updates all the weights in the probability map by performing random
   * Gaussian movements on them.
   * @param localMoveDeviation movement size multiplier (aka Gaussian standard deviation)
   */
  public synchronized void move(double localMoveDeviation) {
    NavigableMap<Double, E> newMap = new TreeMap<Double, E>();
    double runningTotal = 0;
    double newTotal = 0;
    // Iterate over keys in ascending order
    for (Map.Entry<Double, E> entry: map.entrySet()) {
      double newWeight = 0.0;
      double oldWeight = entry.getKey() - runningTotal;
      runningTotal += oldWeight;
      // Check if special condition of fixed weights applies
      if (getMinWeight() == getMaxWeight()) {
        newWeight = getMinWeight();
      } else {
        // Keep generating and testing random Gaussian increments until valid newWeight is found
        do {
          newWeight = oldWeight + random.nextGaussian() * localMoveDeviation;
        } while (newWeight < getMinWeight() || newWeight > getMaxWeight());
      }
      newTotal += newWeight;
      newMap.put(newTotal, entry.getValue());
    }

    // Exchange current values with new total and map representing movements
    total = newTotal;
    map = newMap;
  }

  public double getMoveDeviation() {
    return moveDeviation;
  }

  public void setMoveDeviation(double moveDeviation) {
    this.moveDeviation = moveDeviation;
  }

  public double getMinWeight() {
    return minWeight;
  }

  public void setMinWeight(double minWeight) {
    if (minWeight <= 0 || minWeight > maxWeight) return;
    this.minWeight = minWeight;
  }

  public double getMaxWeight() {
    return maxWeight;
  }

  public void setMaxWeight(double maxWeight) {
    if (maxWeight < minWeight) return;
    this.maxWeight = maxWeight;
  }

  public double getDefaultWeight() {
    return defaultWeight;
  }

  public void setDefaultWeight(double defaultWeight) {
    if (defaultWeight < minWeight || defaultWeight > maxWeight) return;
    this.defaultWeight = defaultWeight;
  }

  public List<Double> getWeights() {
    List<Double> weights = Lists.newArrayList();
    double runningTotal = 0;
    // Iterate over keys in ascending order
    for (Double entryKey: map.keySet()) {
      double weight = entryKey - runningTotal;
      runningTotal += weight;
      weights.add(weight);
    }
    return weights;
  }

  public List<E> getValues() {
    List<E> values = Lists.newArrayList();
    // Iterate over keys in ascending order
    for (E value: map.values()) {
      values.add(value);
    }
    return values;
  }


  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
  }

}
