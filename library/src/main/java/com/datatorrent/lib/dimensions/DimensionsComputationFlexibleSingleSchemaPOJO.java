/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions;

import java.util.Map;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.appdata.gpo.GPOGetters;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;

/**
 * <p>
 * This operator performs dimensions computation on a POJO. See
 * {@link AbstractDimensionsComputationFlexibleSingleSchema}
 * for description of how the dimensions computation is performed.
 * </p>
 * <p>
 * This operator is configured by by setting the getter expressions to use for extracting keys and values from input
 * POJOs.
 * </p>
 *
 * @displayName Dimension Computation
 * @category Stats and Aggregations
 * @tags dimension, aggregation, computation, pojo
 * @since 3.1.0
 */
public class DimensionsComputationFlexibleSingleSchemaPOJO
    extends AbstractDimensionsComputationFlexibleSingleSchema<Object>
{
  /**
   * The array of getters to use to extract keys from input POJOs.
   */
  private transient GPOGetters gpoGettersKey;
  /**
   * The array of getters to use to extract values from input POJOs.
   */
  private transient GPOGetters gpoGettersValue;

  /**
   * Flag indicating whether or not getters need to be created.
   */
  private transient boolean needToCreateGetters = true;
  /**
   * This is a map from a key name (as defined in the
   * {@link com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema}) to the getter expression to use for
   * that key.
   */
  private Map<String, String> keyToExpression;
  /**
   * This is a map from a value name (as defined in the
   * {@link com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema}) to the getter expression to use for
   * that value.
   */
  private Map<String, String> aggregateToExpression;

  public DimensionsComputationFlexibleSingleSchemaPOJO()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
  }

  @Override
  public void convert(InputEvent inputEvent, Object event)
  {
    if (needToCreateGetters) {
      needToCreateGetters = false;

      gpoGettersKey = GPOUtils.buildGPOGetters(keyToExpression,
          configurationSchema.getKeyDescriptorWithTime(),
          event.getClass());

      gpoGettersValue = GPOUtils.buildGPOGetters(aggregateToExpression,
          configurationSchema.getInputValuesDescriptor(),
          event.getClass());
    }

    GPOUtils.copyPOJOToGPO(inputEvent.getKeys(), gpoGettersKey, event);
    GPOUtils.copyPOJOToGPO(inputEvent.getAggregates(), gpoGettersValue, event);
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
  }

  /**
   * @return the keyToExpression
   */
  public Map<String, String> getKeyToExpression()
  {
    return keyToExpression;
  }

  /**
   * @param keyToExpression the keyToExpression to set
   */
  public void setKeyToExpression(Map<String, String> keyToExpression)
  {
    this.keyToExpression = keyToExpression;
  }

  /**
   * @return the aggregateToExpression
   */
  public Map<String, String> getAggregateToExpression()
  {
    return aggregateToExpression;
  }

  /**
   * @param aggregateToExpression the aggregateToExpression to set
   */
  public void setAggregateToExpression(Map<String, String> aggregateToExpression)
  {
    this.aggregateToExpression = aggregateToExpression;
  }
}
