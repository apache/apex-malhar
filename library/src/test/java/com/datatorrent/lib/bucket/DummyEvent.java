/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.bucket;

import javax.annotation.Nonnull;

/**
 * Test event.
 */
public class DummyEvent implements Event, Bucketable, Comparable<DummyEvent>
{
  Integer id;
  long time;

  DummyEvent()
  {
  }

  public DummyEvent(int id, long time)
  {
    this.id = id;
    this.time = time;
  }

  @Override
  public long getTime()
  {
    return time;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DummyEvent)) {
      return false;
    }

    DummyEvent that = (DummyEvent) o;

    if (time != that.time) {
      return false;
    }
    if (id != null ? !id.equals(that.id) : that.id != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (int) (time ^ (time >>> 32));
    return result;
  }

  @Override
  public Object getEventKey()
  {
    return this;
  }

  @Override
  public int compareTo(@Nonnull DummyEvent dummyEvent)
  {
    if (this.equals(dummyEvent)) {
      return 0;
    }
    if (id < dummyEvent.id) {
      return -1;
    }
    return 1;
  }

  @Override
  public String toString()
  {
    return "{id=" + id + ", time=" + time + '}';
  }
}
