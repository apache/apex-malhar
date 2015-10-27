/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions.aggregator;

import com.datatorrent.api.annotation.Name;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.Fields;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * This is the average {@link OTFAggregator}.
 * @since 3.1.0
 *
 */
@Name("AVG")
public class AggregatorAverage implements OTFAggregator
{
  private static final long serialVersionUID = 20154301644L;

  /**
   * The array index of the sum aggregates in the argument list of the {@link #aggregate} function.
   */
  public static int SUM_INDEX = 0;
  /**
   * The array index of the count aggregates in the argument list of the {@link #aggregate} function.
   */
  public static int COUNT_INDEX = 1;
  /**
   * The singleton instance of this class.
   */
  public static final AggregatorAverage INSTANCE = new AggregatorAverage();

  /**
   * The list of {@link IncrementalAggregator}s that this {@link OTFAggregator} depends on.
   */
  public static final transient List<Class<? extends IncrementalAggregator>> CHILD_AGGREGATORS =
  ImmutableList.of(AggregatorIncrementalType.SUM.getAggregator().getClass(),
                   AggregatorIncrementalType.COUNT.getAggregator().getClass());

  /**
   * Constructor for singleton pattern.
   */
  protected AggregatorAverage()
  {
    //Do nothing
  }

  @Override
  public List<Class<? extends IncrementalAggregator>> getChildAggregators()
  {
    return CHILD_AGGREGATORS;
  }

  @Override
  public GPOMutable aggregate(GPOMutable... aggregates)
  {
    Preconditions.checkArgument(aggregates.length == getChildAggregators().size(),
                                "The number of arguments " + aggregates.length +
                                " should be the same as the number of child aggregators " + getChildAggregators().size());

    GPOMutable sumAggregation = aggregates[SUM_INDEX];
    GPOMutable countAggregation = aggregates[COUNT_INDEX];

    FieldsDescriptor fieldsDescriptor = sumAggregation.getFieldDescriptor();
    Fields fields = fieldsDescriptor.getFields();
    GPOMutable result = new GPOMutable(AggregatorUtils.getOutputFieldsDescriptor(fields, this));

    long count = countAggregation.getFieldsLong()[0];

    for(String field: fields.getFields()) {
      Type type = sumAggregation.getFieldDescriptor().getType(field);

      switch(type) {
        case BYTE:
        {
          double val = ((double) sumAggregation.getFieldByte(field)) /
                       ((double) count);
          result.setField(field, val);
          break;
        }
        case SHORT:
        {
          double val = ((double) sumAggregation.getFieldShort(field)) /
                       ((double) count);
          result.setField(field, val);
          break;
        }
        case INTEGER:
        {
          double val = ((double) sumAggregation.getFieldInt(field)) /
                       ((double) count);
          result.setField(field, val);
          break;
        }
        case LONG:
        {
          double val = ((double) sumAggregation.getFieldLong(field)) /
                       ((double) count);
          result.setField(field, val);
          break;
        }
        case FLOAT:
        {
          double val = sumAggregation.getFieldFloat(field) /
                       ((double) count);
          result.setField(field, val);
          break;
        }
        case DOUBLE:
        {
          double val = sumAggregation.getFieldDouble(field) /
                       ((double) count);
          result.setField(field, val);
          break;
        }
        default:
        {
          throw new UnsupportedOperationException("The type " + type + " is not supported.");
        }
      }
    }

    return result;
  }

  @Override
  public Type getOutputType()
  {
    return Type.DOUBLE;
  }
}
