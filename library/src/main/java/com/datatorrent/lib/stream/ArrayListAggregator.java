/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.stream;

import java.util.ArrayList;
import java.util.Collection;

/**
 *
 * Creates a ArrayList tuple from incoming tuples. The size of the ArrayList before it is emitted is determined by property \"size\". If size == 0
 * then the ArrayList (if not empty) is emitted in the endWindow call. Is size is specified then the ArrayList is emitted as soon as the size is
 * reached as part of process(tuple), and no emit happens in endWindow. For size != 0, the operator is stateful.<p>
 * <br>
 * <b>Port</b>:<br>
 * <b>input</b>: expects T<br>
 * <b>output</b>: emits ArrayList&lt;T&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>size</b>: The size of ArrayList. If specified the ArrayList is emitted the moment it reaches this size. If 0, the ArrayList is emitted in endWindow call. Default value is 0, </b>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for DevNull operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 60 million tuples/s</td><td>One tuple (ArrayList) emitted for N (N=3) incoming tuples, where N is the number of keys</td>
 * <td>In-bound rate is the main determinant of performance</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (T=Integer), size = 3</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for DevNull operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th>No Outbound port</th></tr>
 * <tr><th><i>input</i>(V)</th><th><i>output</i>(HashMap&gt;K,V&lt;</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>2</td><td></td></tr>
 * <tr><td>Data (process())</td><td>66</td><td></td></tr>
 * <tr><td>Data (process())</td><td>5</td><td>[2,66,5]</td></tr>
 * <tr><td>Data (process())</td><td>2</td><td></td></tr>
 * <tr><td>Data (process())</td><td>-1</td><td></td></tr>
 * <tr><td>Data (process())</td><td>3</td><td>[2,-1,3]</td></tr>
 * <tr><td>Data (process())</td><td>12</td><td></td></tr>
 * <tr><td>Data (process())</td><td>13</td><td></td></tr>
 * <tr><td>Data (process())</td><td>5</td><td>[12,13,5]</td></tr>
 * <tr><td>Data (process())</td><td>21</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td></td><td></td></tr>
 * <tr><td>Begin Window (beginWindow())</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>4</td><td></td></tr>
 * <tr><td>Data (process())</td><td>5</td><td>[21,4,5]</td></tr>
 * <tr><td>End Window (endWindow())</td><td></td><td></td></tr>
 * </table>
 * <p>
 * <b>Function Table (T=Integer), size = 0</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for DevNull operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>data</i>::process)</th><th>No Outbound port</th></tr>
 * <tr><th><i>input</i>(V)</th><th><i>output</i>(HashMap&gt;K,V&lt;</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td></td><td></td></tr>
 * <tr><td>Data (process())</td><td>2</td><td></td></tr>
 * <tr><td>Data (process())</td><td>66</td><td></td></tr>
 * <tr><td>Data (process())</td><td>5</td><td></td></tr>
 * <tr><td>Data (process())</td><td>2</td><td></td></tr>
 * <tr><td>Data (process())</td><td>-1</td><td></td></tr>
 * <tr><td>Data (process())</td><td>3</td><td></td></tr>
 * <tr><td>Data (process())</td><td>12</td><td></td></tr>
 * <tr><td>Data (process())</td><td>13</td><td></td></tr>
 * <tr><td>Data (process())</td><td>5</td><td></td></tr>
 * <tr><td>Data (process())</td><td>21</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td></td><td>[2,66,5,2,-1,3,12,13,5,21]</td></tr>
 * </table>
 * <br>
 *
 * @param <T> Type of elements in the collection.<br>
 *
 */
public class ArrayListAggregator<T> extends AbstractAggregator<T>
{
  @Override
  public Collection<T> getNewCollection(int size)
  {
    return new ArrayList<T>(size);
  }

}
