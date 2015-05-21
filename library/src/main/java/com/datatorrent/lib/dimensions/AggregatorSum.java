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

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import java.io.Serializable;


public class AggregatorSum implements IncrementalAggregator, Serializable
{
  private static final long serialVersionUID = 20154301649L;

  public AggregatorSum()
  {
  }

  @Override
  public void aggregate(Aggregate dest, Aggregate src)
  {
    aggregateHelper(dest, src);
  }

  @Override
  public void aggregate(Aggregate dest, InputEvent src)
  {
    aggregateHelper(dest, src);
  }

  @Override
  public Type getOutputType(Type inputType)
  {
    return AggregatorUtils.IDENTITY_NUMBER_TYPE_MAP.get(inputType);
  }

  @Override
  public Aggregate createDest(InputEvent inputEvent)
  {
    return new Aggregate(inputEvent.getEventKey(), inputEvent.getAggregates());
  }

  private void aggregateHelper(DimensionsEvent dest, DimensionsEvent src)
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
          destByte[index] += srcByte[index];
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
          destShort[index] += srcShort[index];
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
          destInteger[index] += srcInteger[index];
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
          destLong[index] += srcLong[index];
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
          destFloat[index] += srcFloat[index];
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
          destDouble[index] += srcDouble[index];
        }
      }
    }
  }
}
