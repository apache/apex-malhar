/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions.aggregator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.annotation.Name;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;

/**
 * This {@link IncrementalAggregator} performs a sum operation over the fields in the given {@link InputEvent}.
 *
 * @since 3.1.0
 */
@Name("SUM")
public class AggregatorSum extends AbstractIncrementalAggregator
{
  private static final long serialVersionUID = 20154301649L;

  public AggregatorSum()
  {
    //Do nothing
  }

  @Override
  public Aggregate getGroup(InputEvent src, int aggregatorIndex)
  {
    src.used = true;
    Aggregate aggregate = createAggregate(src,
        context,
        aggregatorIndex);

    GPOMutable value = aggregate.getAggregates();
    GPOUtils.zeroFillNumeric(value);

    return aggregate;
  }

  @Override
  public void aggregate(Aggregate dest, Aggregate src)
  {
    GPOMutable destAggs = dest.getAggregates();
    GPOMutable srcAggs = src.getAggregates();

    aggregateAggs(destAggs, srcAggs);
  }

  public void aggregateAggs(GPOMutable destAggs, GPOMutable srcAggs)
  {
    {
      byte[] destByte = destAggs.getFieldsByte();
      if (destByte != null) {
        byte[] srcByte = srcAggs.getFieldsByte();

        for (int index = 0;
            index < destByte.length;
            index++) {
          destByte[index] += srcByte[index];
        }
      }
    }

    {
      short[] destShort = destAggs.getFieldsShort();
      if (destShort != null) {
        short[] srcShort = srcAggs.getFieldsShort();

        for (int index = 0;
            index < destShort.length;
            index++) {
          destShort[index] += srcShort[index];
        }
      }
    }

    {
      int[] destInteger = destAggs.getFieldsInteger();
      if (destInteger != null) {
        int[] srcInteger = srcAggs.getFieldsInteger();

        for (int index = 0;
            index < destInteger.length;
            index++) {
          destInteger[index] += srcInteger[index];
        }
      }
    }

    {
      long[] destLong = destAggs.getFieldsLong();
      if (destLong != null) {
        long[] srcLong = srcAggs.getFieldsLong();

        for (int index = 0;
            index < destLong.length;
            index++) {
          destLong[index] += srcLong[index];
        }
      }
    }

    {
      float[] destFloat = destAggs.getFieldsFloat();
      if (destFloat != null) {
        float[] srcFloat = srcAggs.getFieldsFloat();

        for (int index = 0;
            index < destFloat.length;
            index++) {
          destFloat[index] += srcFloat[index];
        }
      }
    }

    {
      double[] destDouble = destAggs.getFieldsDouble();
      if (destDouble != null) {
        double[] srcDouble = srcAggs.getFieldsDouble();

        for (int index = 0;
            index < destDouble.length;
            index++) {
          destDouble[index] += srcDouble[index];
        }
      }
    }
  }

  @Override
  public void aggregate(Aggregate dest, InputEvent src)
  {
    GPOMutable destAggs = dest.getAggregates();
    GPOMutable srcAggs = src.getAggregates();

    aggregateInput(destAggs, srcAggs);
  }

  public void aggregateInput(GPOMutable destAggs, GPOMutable srcAggs)
  {
    {
      byte[] destByte = destAggs.getFieldsByte();
      if (destByte != null) {
        byte[] srcByte = srcAggs.getFieldsByte();
        int[] srcIndices = context.indexSubsetAggregates.fieldsByteIndexSubset;
        for (int index = 0;
            index < destByte.length;
            index++) {
          destByte[index] += srcByte[srcIndices[index]];
        }
      }
    }

    {
      short[] destShort = destAggs.getFieldsShort();
      if (destShort != null) {
        short[] srcShort = srcAggs.getFieldsShort();
        int[] srcIndices = context.indexSubsetAggregates.fieldsShortIndexSubset;
        for (int index = 0;
            index < destShort.length;
            index++) {
          destShort[index] += srcShort[srcIndices[index]];
        }
      }
    }

    {
      int[] destInteger = destAggs.getFieldsInteger();
      if (destInteger != null) {
        int[] srcInteger = srcAggs.getFieldsInteger();
        int[] srcIndices = context.indexSubsetAggregates.fieldsIntegerIndexSubset;
        for (int index = 0;
            index < destInteger.length;
            index++) {
          destInteger[index] += srcInteger[srcIndices[index]];
        }
      }
    }

    {
      long[] destLong = destAggs.getFieldsLong();
      if (destLong != null) {
        long[] srcLong = srcAggs.getFieldsLong();
        int[] srcIndices = context.indexSubsetAggregates.fieldsLongIndexSubset;
        for (int index = 0;
            index < destLong.length;
            index++) {
          destLong[index] += srcLong[srcIndices[index]];
        }
      }
    }

    {
      float[] destFloat = destAggs.getFieldsFloat();
      if (destFloat != null) {
        float[] srcFloat = srcAggs.getFieldsFloat();
        int[] srcIndices = context.indexSubsetAggregates.fieldsFloatIndexSubset;
        for (int index = 0;
            index < destFloat.length;
            index++) {
          destFloat[index] += srcFloat[srcIndices[index]];
        }
      }
    }

    {
      double[] destDouble = destAggs.getFieldsDouble();
      if (destDouble != null) {
        double[] srcDouble = srcAggs.getFieldsDouble();
        int[] srcIndices = context.indexSubsetAggregates.fieldsDoubleIndexSubset;
        for (int index = 0;
            index < destDouble.length;
            index++) {
          destDouble[index] += srcDouble[srcIndices[index]];
        }
      }
    }
  }

  @Override
  public Type getOutputType(Type inputType)
  {
    return AggregatorUtils.IDENTITY_NUMBER_TYPE_MAP.get(inputType);
  }

  @Override
  public FieldsDescriptor getMetaDataDescriptor()
  {
    return null;
  }

  private static final Logger LOG = LoggerFactory.getLogger(AggregatorSum.class);
}
