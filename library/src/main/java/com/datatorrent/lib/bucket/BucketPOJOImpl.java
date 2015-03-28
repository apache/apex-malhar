/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

import java.util.List;

public class BucketPOJOImpl extends AbstractBucket<SimpleEvent>
{
  // id in Simple class

  protected BucketPOJOImpl(long requestedKey)
  {
    super(requestedKey);
  }


  /**
   * Finds whether the bucket contains the event.
   *
   * @param event the {@link Bucketable} to search for in the bucket.
   * @return true if bucket has the event; false otherwise.
   */
  @Override
  public boolean containsEvent(SimpleEvent event)
  {
    if (unwrittenEvents != null) {
    //  for (String key: keys) {
        if (!unwrittenEvents.containsKey(event.id)) {
          return false;
        }
     // }
      return true;

    }
    if (writtenEvents != null) {
     // for (String key: keys) {
         if (!writtenEvents.containsKey(event.id))  {
          return false;
        }
      //}
      return true;
    }
    return false;

  }

}
