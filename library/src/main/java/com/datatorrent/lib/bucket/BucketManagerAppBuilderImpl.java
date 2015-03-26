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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BucketManagerAppBuilderImpl extends AbstractBucketManager<HashMap<String,Object>>
{

  private static transient final Logger logger = LoggerFactory.getLogger(BucketManagerAppBuilderImpl.class);

  @NotNull
  protected BucketableCustomKey customKey;

  public ArrayList<Object> getCustomKey()
  {
    return customKey.key;
  }

  public void setCustomKey(BucketableCustomKey customKey)
  {
    this.customKey = customKey;
  }

  @Override
  protected Object getEventKey(HashMap<String,Object> event)
  {
    return event.get(customKey.getEventKey());
  }

  @Override
  protected AbstractBucketManager<HashMap<String, Object>> getBucketManagerImpl()
  {
    return new BucketManagerAppBuilderImpl();
  }

  @Override
  protected boolean checkInstanceOfBucketManager(Object o)
  {
    if(!(o instanceof BucketManagerAppBuilderImpl))
    return false;
    else
      return true;
  }

  @Override
  public long getBucketKeyFor(HashMap<String,Object> event)
  {
    return Math.abs(event.get(customKey.getEventKey()).hashCode()) / noOfBuckets;
  }

  @Override
  protected Bucket<HashMap<String, Object>> createBucket(long requestedKey)
  {
    return new BucketAppBuilderImpl(requestedKey);
  }


}
