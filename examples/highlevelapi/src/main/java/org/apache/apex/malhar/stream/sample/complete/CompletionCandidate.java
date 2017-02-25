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
package org.apache.apex.malhar.stream.sample.complete;

/**
 * Class used to store tag-count pairs in Auto Complete Example.
 *
 * @since 3.5.0
 */
public class CompletionCandidate implements Comparable<CompletionCandidate>
{
  private long count;
  private String value;

  public CompletionCandidate(String value, long count)
  {
    this.value = value;
    this.count = count;
  }

  public long getCount()
  {
    return count;
  }

  public String getValue()
  {
    return value;
  }

  // Empty constructor required for Kryo.
  public CompletionCandidate()
  {

  }

  @Override
  public int compareTo(CompletionCandidate o)
  {
    if (this.count < o.count) {
      return -1;
    } else if (this.count == o.count) {
      return this.value.compareTo(o.value);
    } else {
      return 1;
    }
  }

  @Override
  public boolean equals(Object other)
  {
    if (other instanceof CompletionCandidate) {
      CompletionCandidate that = (CompletionCandidate)other;
      return this.count == that.count && this.value.equals(that.value);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode()
  {
    return Long.valueOf(count).hashCode() ^ value.hashCode();
  }

  @Override
  public String toString()
  {
    return "CompletionCandidate[" + value + ", " + count + "]";
  }
}


