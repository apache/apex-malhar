/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.lib.datamodel.operation;

import javax.annotation.Nullable;

public class SumOperation implements Operation
{
  @Override
  public Object compute(@Nullable Object last, @Nullable Object value)
  {
    Double lastDouble = (Double) last;
    Number valueNumber = (Number) value;

    return (lastDouble == null ? 0.0 : lastDouble.doubleValue()) + (valueNumber == null ? 0.0 : valueNumber.doubleValue());
  }
}
