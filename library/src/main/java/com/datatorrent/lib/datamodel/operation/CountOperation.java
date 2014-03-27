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
 */
public class CountOperation<OUTPUT extends Number> implements Operation<OUTPUT, OUTPUT>
{

  @Override
  public OUTPUT compute(OUTPUT last, OUTPUT value)
  {
    if (last instanceof Double) {
      return (OUTPUT) (new Double(last.doubleValue() + 1));
    }
    else if (last instanceof Float) {
      return (OUTPUT) (new Float(last.floatValue() + 1));
    }
    else if (last instanceof Long) {
      return (OUTPUT) (new Long(last.longValue() + 1));
    }
    else {
      return (OUTPUT) (new Integer(last.intValue() + 1));
    }
  }

}
