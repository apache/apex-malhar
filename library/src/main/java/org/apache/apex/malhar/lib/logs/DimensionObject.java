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
package org.apache.apex.malhar.lib.logs;

import org.apache.commons.lang.mutable.MutableDouble;

/**
 * <p>
 * DimensionObject class.
 * </p>
 * This is the object that is passed to TopNOperator
 *
 * @since 0.3.4
 */
public class DimensionObject<T> implements Comparable<DimensionObject<T>>
{

  private MutableDouble count;
  private T val;

  @SuppressWarnings("unused")
  private DimensionObject()
  {

  }

  public DimensionObject(MutableDouble count, T s)
  {
    this.count = count;
    val = s;
  }

  @Override
  public String toString()
  {
    return count + "," + val.toString();
  }

  @Override
  public int compareTo(DimensionObject<T> arg0)
  {
    return count.compareTo(arg0.count);
  }

  public MutableDouble getCount()
  {
    return count;
  }

  public void setCount(MutableDouble count)
  {
    this.count = count;
  }

  public T getVal()
  {
    return val;
  }

  public void setVal(T val)
  {
    this.val = val;
  }

  @Override
  public int hashCode()
  {
    return (val.toString() + count.toString()).hashCode();
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null) {
      return false;
    }
    if (!this.getClass().equals(obj.getClass())) {
      return false;
    }
    @SuppressWarnings("unchecked")
    DimensionObject<T> obj2 = (DimensionObject<T>)obj;
    return this.val.equals(obj2.val);

  }

}
