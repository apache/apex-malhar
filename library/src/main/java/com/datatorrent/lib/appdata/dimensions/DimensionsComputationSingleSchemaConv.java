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

import com.datatorrent.lib.converter.Converter;
import com.google.common.base.Preconditions;
import javax.validation.constraints.NotNull;

public abstract class DimensionsComputationSingleSchemaConv<INPUT_EVENT> extends DimensionsComputationSingleSchema<INPUT_EVENT>
{
  @NotNull
  protected Converter<INPUT_EVENT, AggregateEvent, DimensionsConversionContext> converter;

  public DimensionsComputationSingleSchemaConv()
  {
  }

  public void setConverter(Converter<INPUT_EVENT, AggregateEvent, DimensionsConversionContext> converter)
  {
    this.converter = Preconditions.checkNotNull(converter, "converter");
  }

  public Converter<INPUT_EVENT, AggregateEvent, DimensionsConversionContext> getConverter()
  {
    return converter;
  }

  @Override
  public AggregateEvent createGenericAggregateEvent(INPUT_EVENT inputEvent, DimensionsConversionContext conversionContext)
  {
    return converter.convert(inputEvent, conversionContext);
  }
}
