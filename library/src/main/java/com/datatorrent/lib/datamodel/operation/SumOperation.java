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

/**
 * @param <OUTPUT>
 * @param <INPUT>
 */
public class SumOperation<OUTPUT extends Number, INPUT extends Number> implements Operation<OUTPUT, INPUT>
{

  @SuppressWarnings("unchecked")
  @Override
  public OUTPUT compute(OUTPUT last, INPUT value)
  {
    if (last instanceof Double) {
      Double sum = last.doubleValue() + value.doubleValue();
      return (OUTPUT) sum;
    }
    else if (last instanceof Float) {
      Float sum = last.floatValue() + value.floatValue();
      return (OUTPUT) sum;
    }
    else if (last instanceof Long) {
      Long sum = last.longValue() + value.longValue();
      return (OUTPUT) sum;
    }
    else {
      Integer sum = last.intValue() + value.intValue();
      return (OUTPUT) sum;
    }
  }
}
