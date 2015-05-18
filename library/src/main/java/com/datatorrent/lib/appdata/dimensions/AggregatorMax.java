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

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import java.io.Serializable;


public class AggregatorMax implements DimensionsStaticAggregator, Serializable
{
  private static final long serialVersionUID = 201503120332L;

  public AggregatorMax()
  {
  }

  @Override
  public void aggregate(AggregateEvent dest, AggregateEvent src)
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
          if(destByte[index] < srcByte[index]) {
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
          if(destShort[index] < srcShort[index]) {
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
          if(destInteger[index] < srcInteger[index]) {
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
          if(destLong[index] < srcLong[index]) {
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
          if(destFloat[index] < srcFloat[index]) {
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
          if(destDouble[index] < srcDouble[index]) {
            destDouble[index] = srcDouble[index];
          }
        }
      }
    }
  }

  @Override
  public AggregatorTypeMap getTypeMap()
  {
    return AggregatorUtils.IDENTITY_NUMBER_TYPE_MAP;
  }

  @Override
  public FieldsDescriptor getResultDescriptor(FieldsDescriptor fd)
  {
    if(!Type.NUMERIC_TYPES.containsAll(fd.getTypes())) {
      throw new UnsupportedOperationException("The given field descriptor can only contain numeric types.");
    }

    return fd;
  }

  @Override
  public AggregateEvent createDest(AggregateEvent first, FieldsDescriptor fd)
  {
    return first;
  }

  @Override
  public void aggregateAggs(AggregateEvent dest, AggregateEvent src)
  {
    aggregate(dest, src);
  }
}
