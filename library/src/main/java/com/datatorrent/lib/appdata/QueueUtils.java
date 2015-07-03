/*
 * Copyright (c) 2015 DataTorrent, Inc.
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
package com.datatorrent.lib.appdata;

public class QueueUtils
{
  /**
   * This class should not be instantiated.
   */
  private QueueUtils()
  {
    //Do nothing
  }

  public static class ConditionBarrier
  {
    private boolean locked = false;
    private final Object lock = new Object();

    public ConditionBarrier()
    {
    }

    public void lock()
    {
      synchronized(lock) {
        locked = true;
      }
    }

    public void unlock()
    {
      synchronized(lock) {
        locked = false;
        lock.notifyAll();
      }
    }

    public void gate()
    {
      synchronized(lock) {
        while(locked) {
          try {
            lock.wait();
          }
          catch(InterruptedException ex) {
            //Do nothing
          }
        }
      }
    }
  }
}
