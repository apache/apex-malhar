/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.utils;

import org.apache.apex.malhar.lib.window.Tuple;

/**
 * The tuple util will be used to extract fields that are used as key or value<br>
 * Or converting from data tuples to display tuples <br>
 * Or generating watermark tuples <br>
 *
 *
 * @since 3.4.0
 */
public class TupleUtil
{

  public static <T, O> Tuple.PlainTuple<O> buildOf(Tuple.PlainTuple<T> t, O newValue)
  {

    if (t instanceof Tuple.WindowedTuple) {
      Tuple.WindowedTuple windowedTuple = (Tuple.WindowedTuple)t;
      return new Tuple.WindowedTuple<>(windowedTuple.getWindows(), windowedTuple.getTimestamp(), newValue);
    } else if (t instanceof Tuple.TimestampedTuple) {
      return new Tuple.TimestampedTuple<>(((Tuple.TimestampedTuple)t).getTimestamp(), newValue);
    } else {
      return new Tuple.PlainTuple<>(newValue);
    }
  }
}
