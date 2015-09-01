/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.dimensions.aggregator;

import com.datatorrent.api.annotation.Name;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;

/**
 * This {@link IncrementalAggregator} takes the min of the fields provided in the {@link InputEvent}.
 * @since 3.1.0
 *
 */
@Name("MIN")
public class AggregatorMin extends AbstractIncrementalAggregator
{
  private static final long serialVersionUID = 20154301648L;

  public AggregatorMin()
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
  public void aggregate(Aggregate dest, InputEvent src)
  {
    GPOMutable destAggs = dest.getAggregates();
    GPOMutable srcAggs = src.getAggregates();

    {
      byte[] destByte = destAggs.getFieldsByte();
      if(destByte != null) {
        byte[] srcByte = srcAggs.getFieldsByte();
        int[] srcIndices = context.indexSubsetAggregates.fieldsByteIndexSubset;
        for(int index = 0;
            index < destByte.length;
            index++) {
          byte tempVal = srcByte[srcIndices[index]];
          if(destByte[index] > tempVal) {
            destByte[index] = tempVal;
          }
        }
      }
    }

    {
      short[] destShort = destAggs.getFieldsShort();
      if(destShort != null) {
        short[] srcShort = srcAggs.getFieldsShort();
        int[] srcIndices = context.indexSubsetAggregates.fieldsShortIndexSubset;
        for(int index = 0;
            index < destShort.length;
            index++) {
          short tempVal = srcShort[srcIndices[index]];
          if(destShort[index] > tempVal) {
            destShort[index] = tempVal;
          }
        }
      }
    }

    {
      int[] destInteger = destAggs.getFieldsInteger();
      if(destInteger != null) {
        int[] srcInteger = srcAggs.getFieldsInteger();
        int[] srcIndices = context.indexSubsetAggregates.fieldsIntegerIndexSubset;
        for(int index = 0;
            index < destInteger.length;
            index++) {
          int tempVal = srcInteger[srcIndices[index]];
          if(destInteger[index] > tempVal) {
            destInteger[index] = tempVal;
          }
        }
      }
    }

    {
      long[] destLong = destAggs.getFieldsLong();
      if(destLong != null) {
        long[] srcLong = srcAggs.getFieldsLong();
        int[] srcIndices = context.indexSubsetAggregates.fieldsLongIndexSubset;
        for(int index = 0;
            index < destLong.length;
            index++) {
          long tempVal = srcLong[srcIndices[index]];
          if(destLong[index] > tempVal) {
            destLong[index] = tempVal;
          }
        }
      }
    }

    {
      float[] destFloat = destAggs.getFieldsFloat();
      if(destFloat != null) {
        float[] srcFloat = srcAggs.getFieldsFloat();
        int[] srcIndices = context.indexSubsetAggregates.fieldsFloatIndexSubset;
        for(int index = 0;
            index < destFloat.length;
            index++) {
          float tempVal = srcFloat[srcIndices[index]];
          if(destFloat[index] > tempVal) {
            destFloat[index] = tempVal;
          }
        }
      }
    }

    {
      double[] destDouble = destAggs.getFieldsDouble();
      if(destDouble != null) {
        double[] srcDouble = srcAggs.getFieldsDouble();
        int[] srcIndices = context.indexSubsetAggregates.fieldsDoubleIndexSubset;
        for(int index = 0;
            index < destDouble.length;
            index++) {
          double tempVal = srcDouble[srcIndices[index]];
          if(destDouble[index] > tempVal) {
            destDouble[index] = tempVal;
          }
        }
      }
    }
  }

  @Override
  public void aggregate(Aggregate dest, Aggregate src)
  {
    GPOMutable destAggs = dest.getAggregates();
    GPOMutable srcAggs = src.getAggregates();

    {
      byte[] destByte = destAggs.getFieldsByte();
      if(destByte != null) {
        byte[] srcByte = srcAggs.getFieldsByte();

        for(int index = 0;
            index < destByte.length;
            index++) {
          if(destByte[index] > srcByte[index]) {
            destByte[index] = srcByte[index];
          }
        }
      }
    }

    {
      short[] destShort = destAggs.getFieldsShort();
      if(destShort != null) {
        short[] srcShort = srcAggs.getFieldsShort();

        for(int index = 0;
            index < destShort.length;
            index++) {
          if(destShort[index] > srcShort[index]) {
            destShort[index] = srcShort[index];
          }
        }
      }
    }

    {
      int[] destInteger = destAggs.getFieldsInteger();
      if(destInteger != null) {
        int[] srcInteger = srcAggs.getFieldsInteger();

        for(int index = 0;
            index < destInteger.length;
            index++) {
          if(destInteger[index] > srcInteger[index]) {
            destInteger[index] = srcInteger[index];
          }
        }
      }
    }

    {
      long[] destLong = destAggs.getFieldsLong();
      if(destLong != null) {
        long[] srcLong = srcAggs.getFieldsLong();

        for(int index = 0;
            index < destLong.length;
            index++) {
          if(destLong[index] > srcLong[index]) {
            destLong[index] = srcLong[index];
          }
        }
      }
    }

    {
      float[] destFloat = destAggs.getFieldsFloat();
      if(destFloat != null) {
        float[] srcFloat = srcAggs.getFieldsFloat();

        for(int index = 0;
            index < destFloat.length;
            index++) {
          if(destFloat[index] > srcFloat[index]) {
            destFloat[index] = srcFloat[index];
          }
        }
      }
    }

    {
      double[] destDouble = destAggs.getFieldsDouble();
      if(destDouble != null) {
        double[] srcDouble = srcAggs.getFieldsDouble();

        for(int index = 0;
            index < destDouble.length;
            index++) {
          if(destDouble[index] > srcDouble[index]) {
            destDouble[index] = srcDouble[index];
          }
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
}
