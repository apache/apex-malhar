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
import com.datatorrent.lib.util.PojoUtils.GetterObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of AbstractBucket which can take in any POJO.
 * @displayName: BucketPOJOImplementation
 *
 * @since 2.1.0
 */
public class BucketPOJOImpl extends AbstractBucket<Object>
{
  private String expression;
  private transient GetterObject getter;

  protected BucketPOJOImpl(long bucketKey,String expression)
  {
    super(bucketKey);
    this.expression = expression;
  }

  @Override
  protected Object getEventKey(Object event)
  {
    if(getter==null){
      Class<?> fqcn = event.getClass();
      GetterObject getterObj = PojoUtils.createGetterObject(fqcn, expression);
      getter = getterObj;
    }
    return getter.get(event);
  }

  private final static Logger logger = LoggerFactory.getLogger(BucketPOJOImpl.class);

}
