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
package org.apache.apex.malhar.lib.stream;

import java.util.ArrayList;
import java.util.Collection;

/**
 *
 * An implementation of AbstractAggregator that creates an ArrayList tuple from incoming tuples.
 * <p>
 * The size of the ArrayList before it is emitted is determined by property \"size\". If size == 0
 * then the ArrayList (if not empty) is emitted in the endWindow call. Is size is specified then the ArrayList is emitted as soon as the size is
 * reached as part of process(tuple), and no emit happens in endWindow. For size != 0, the operator is statefull.<p>
 * <br>
 * <b>Port</b>:<br>
 * <b>input</b>: expects T<br>
 * <b>output</b>: emits ArrayList&lt;T&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>size</b>: The size of ArrayList. If specified the ArrayList is emitted the moment it reaches this size.
 *               If 0, the ArrayList is emitted in endWindow call. Default value is 0, </br>
 * <br>
 * @displayName Array List Aggregator
 * @category Stream Manipulators
 * @tags list, aggregate, collection
 * @param <T> Type of elements in the collection.<br>
 * @since 0.3.3
 */
public class ArrayListAggregator<T> extends AbstractAggregator<T>
{
  @Override
  public Collection<T> getNewCollection(int size)
  {
    return new ArrayList<T>(size);
  }
}
