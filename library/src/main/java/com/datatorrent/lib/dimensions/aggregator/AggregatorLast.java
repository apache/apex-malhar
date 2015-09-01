/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions.aggregator;

import com.datatorrent.api.annotation.Name;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.dimensions.DimensionsEvent;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;

/**
 * <p>
 * This aggregator creates an aggregate out of the last {@link InputEvent} encountered by this aggregator. All previous
 * {@link InputEvent}s are ignored.
 * </p>
 * <p>
 * <b>Note:</b> when aggregates are combined in a unifier it is not possible to tell which came first or last, so
 * one is picked arbitrarily to be the last.
 * </p>
 * @since 3.1.0
 *
 */
@Name("LAST")
public class AggregatorLast extends AbstractIncrementalAggregator
{
  private static final long serialVersionUID = 20154301647L;

  public AggregatorLast()
  {
    //Do nothing
  }

  @Override
  public Aggregate getGroup(InputEvent src, int aggregatorIndex)
  {
    Aggregate aggregate = super.getGroup(src, aggregatorIndex);

    GPOUtils.indirectCopy(aggregate.getAggregates(), src.getAggregates(), context.indexSubsetAggregates);

    return aggregate;
  }

  @Override
  public Type getOutputType(Type inputType)
  {
    return AggregatorUtils.IDENTITY_TYPE_MAP.get(inputType);
  }

  @Override
  public void aggregate(Aggregate dest, InputEvent src)
  {
    GPOUtils.indirectCopy(dest.getAggregates(), src.getAggregates(), context.indexSubsetAggregates);
  }

  @Override
  public void aggregate(Aggregate dest, Aggregate src)
  {
    DimensionsEvent.copy(dest, src);
  }

  @Override
  public FieldsDescriptor getMetaDataDescriptor()
  {
    return null;
  }
}
