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

import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;

public class AggregatorFirst implements DimensionsStaticAggregator
{
  public AggregatorFirst()
  {
  }

  @Override
  public AggregatorTypeMap getTypeMap()
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
    //Do nothing
  }
}
