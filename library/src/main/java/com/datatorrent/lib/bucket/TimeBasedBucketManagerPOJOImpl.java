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

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.GetterLong;

import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BucketManager} that creates buckets based on time.<br/>
 *
 * @displayName: TimeBasedBucketManagerPOJOImplemenation
 *
 * @since 2.1.0
 */
public class TimeBasedBucketManagerPOJOImpl extends AbstractTimeBasedBucketManager<Object> implements POJOBucketManager<Object>
{
  @NotNull
  private String timeExpression;
  @NotNull
  private String keyExpression;

  /*
   * A Java expression that will yield the deduper key from the POJO.
   */
  public String getKeyExpression()
  {
    return keyExpression;
  }

  public void setKeyExpression(String keyExpression)
  {
    this.keyExpression = keyExpression;
  }

  private transient GetterLong getter;

  /*
   * A Java expression that will yield the timestamp value from the POJO.
   * This expression needs to evaluate to long
   */
  public String getTimeExpression()
  {
    return timeExpression;
  }

  /*
   * Sets the Java expression that will yield the timestamp value from the POJO.
   * This expression needs to evaluate to long
   */
  public void setTimeExpression(String timeExpression)
  {
    this.timeExpression = timeExpression;
  }


  @Override
  protected BucketPOJOImpl createBucket(long bucketKey)
  {
    return new BucketPOJOImpl(bucketKey,keyExpression);
  }

  @Override
  protected long getTime(Object event)
  {
    if(getter==null){
    Class<?> fqcn = event.getClass();
    GetterLong getterTime = PojoUtils.createGetterLong(fqcn, timeExpression);
    getter = getterTime;
    }
    return getter.get(event);
  }

  private static transient final Logger logger = LoggerFactory.getLogger(TimeBasedBucketManagerPOJOImpl.class);

}
