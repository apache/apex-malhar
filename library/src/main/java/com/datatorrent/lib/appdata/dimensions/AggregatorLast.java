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

import java.util.Map;

public class AggregatorLast implements DimensionsAggregator
{
  public AggregatorLast()
  {
  }

  @Override
  public Map<Type, Type> getTypeConversionMap()
  {
    return AggregatorUtils.IDENTITY_TYPE_MAP;
  }

  @Override
  public FieldsDescriptor getResultDescriptor(FieldsDescriptor fd)
  {
    return fd;
  }

  @Override
  public AggregateEvent createDest(AggregateEvent first, FieldsDescriptor fd)
  {
    return first;
  }

  @Override
  public void aggregate(AggregateEvent dest, AggregateEvent src)
  {
    GPOMutable destAggs = dest.getAggregates();
    GPOMutable srcAggs = src.getAggregates();

    System.arraycopy(srcAggs.getFieldsBoolean(), 0, destAggs.getFieldsBoolean(), 0, srcAggs.getFieldsBoolean().length);
    System.arraycopy(srcAggs.getFieldsCharacter(), 0, destAggs.getFieldsCharacter(), 0, srcAggs.getFieldsCharacter().length);
    System.arraycopy(srcAggs.getFieldsString(), 0, destAggs.getFieldsString(), 0, srcAggs.getFieldsString().length);

    System.arraycopy(srcAggs.getFieldsShort(), 0, destAggs.getFieldsShort(), 0, srcAggs.getFieldsShort().length);
    System.arraycopy(srcAggs.getFieldsInteger(), 0, destAggs.getFieldsInteger(), 0, srcAggs.getFieldsInteger().length);
    System.arraycopy(srcAggs.getFieldsLong(), 0, destAggs.getFieldsLong(), 0, srcAggs.getFieldsLong().length);

    System.arraycopy(srcAggs.getFieldsFloat(), 0, destAggs.getFieldsFloat(), 0, srcAggs.getFieldsFloat().length);
    System.arraycopy(srcAggs.getFieldsDouble(), 0, destAggs.getFieldsDouble(), 0, srcAggs.getFieldsDouble().length);
  }
}
