/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions;

import java.util.Map;

import javax.validation.constraints.NotNull;

import com.google.common.collect.Maps;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;

import com.datatorrent.api.Context.OperatorContext;

/**
 * <p>
 * This operator performs dimensions computation on a map. See {@link AbstractDimensionsComputationFlexibleSingleSchema}
 * for description of how the dimensions computation is performed.
 * </p>
 * <p>
 * This operator is configured by by setting key and value aliases. These aliases are used in the
 * case where the names of keys or values as they're defined in the {@link DimensionalConfigurationSchema} are different from the
 * names of keys or values as they're defined in incoming input maps.
 * </p>
 * @displayName Dimension Computation Map
 * @category Stats and Aggregations
 * @tags event, dimension, aggregation, computation, map
 * @since 3.1.0
 *  
 */
public class DimensionsComputationFlexibleSingleSchemaMap extends AbstractDimensionsComputationFlexibleSingleSchema<Map<String, Object>>
{
  @NotNull
  private Map<String, String> keyNameAliases = Maps.newHashMap();
  @NotNull
  private Map<String, String> valueNameAliases = Maps.newHashMap();

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
  }

  @Override
  public void convert(InputEvent inputEvent, Map<String, Object> event)
  {
    populateGPO(inputEvent.getKeys(),
                event,
                keyNameAliases);

    populateGPO(inputEvent.getAggregates(),
                event,
                valueNameAliases);
  }

  private void populateGPO(GPOMutable gpoMutable,
                           Map<String, Object> event,
                           Map<String, String> aliasMap)
  {
    FieldsDescriptor fd = gpoMutable.getFieldDescriptor();

    for(int index = 0;
        index < fd.getFieldList().size();
        index++) {
      String field = fd.getFieldList().get(index);
      gpoMutable.setFieldGeneric(field, event.get(getMapAlias(aliasMap,
                                                     field)));
    }
  }

  private String getMapAlias(Map<String, String> map,
                             String name)
  {
    String aliasName = map.get(name);

    if(aliasName == null) {
      aliasName = name;
    }

    return aliasName;
  }

  /**
   * Gets the keyNameAliases.
   * @return The keyNameAliases.
   */
  public Map<String, String> getKeyNameAliases()
  {
    return keyNameAliases;
  }

  /**
   * Sets a map from key names as defined in the {@link DimensionalConfigurationSchema} to the
   * corresponding key names in input maps.
   * @param keyNameAliases The keyNameAliases to set.
   */
  public void setKeyNameAliases(Map<String, String> keyNameAliases)
  {
    this.keyNameAliases = keyNameAliases;
  }

  /**
   * Gets the valueNameAliases.
   * @return The valueNameAliases.
   */
  public Map<String, String> getValueNameAliases()
  {
    return valueNameAliases;
  }

  /**
   * Sets a map from value names as defined in the {@link DimensionalConfigurationSchema} to the
   * corresponding value names in input maps.
   * @param valueNameAliases The valueNameAliases to set.
   */
  public void setValueNameAliases(Map<String, String> valueNameAliases)
  {
    this.valueNameAliases = valueNameAliases;
  }
}
