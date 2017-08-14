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
package org.apache.apex.malhar.lib.dimensions;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.appdata.schemas.CustomTimeBucket;
import org.apache.apex.malhar.lib.appdata.schemas.Fields;
import org.apache.apex.malhar.lib.appdata.schemas.TimeBucket;
import org.apache.apex.malhar.lib.appdata.schemas.Type;

import com.google.common.collect.Sets;

public class DimensionsDescriptorTest
{
  public static final String KEY_1_NAME = "key1";
  public static final Type KEY_1_TYPE = Type.INTEGER;
  public static final String KEY_2_NAME = "key2";
  public static final Type KEY_2_TYPE = Type.STRING;

  public static final String AGG_1_NAME = "agg1";
  public static final Type AGG_1_TYPE = Type.INTEGER;
  public static final String AGG_2_NAME = "agg2";
  public static final Type AGG_2_TYPE = Type.STRING;

  @Test
  public void simpleTest1()
  {
    DimensionsDescriptor ad = new DimensionsDescriptor(KEY_1_NAME);

    Set<String> fields = Sets.newHashSet();
    fields.add(KEY_1_NAME);

    Assert.assertEquals("The fields should match.", fields, ad.getFields().getFields());
    Assert.assertEquals("The timeunit should be null.", null, ad.getTimeBucket());
  }

  @Test
  public void simpleTest2()
  {
    DimensionsDescriptor ad = new DimensionsDescriptor(KEY_1_NAME + DimensionsDescriptor.DELIMETER_SEPERATOR +
        KEY_2_NAME);

    Set<String> fields = Sets.newHashSet();
    fields.add(KEY_1_NAME);
    fields.add(KEY_2_NAME);

    Assert.assertEquals("The fields should match.", fields, ad.getFields().getFields());
    Assert.assertEquals("The timeunit should be null.", null, ad.getTimeBucket());
  }

  @Test
  public void simpleTimeTest()
  {
    DimensionsDescriptor ad = new DimensionsDescriptor(KEY_1_NAME + DimensionsDescriptor.DELIMETER_SEPERATOR +
        DimensionsDescriptor.DIMENSION_TIME + DimensionsDescriptor.DELIMETER_EQUALS + "DAYS");

    Set<String> fields = Sets.newHashSet();
    fields.add(KEY_1_NAME);

    Assert.assertEquals("The fields should match.", fields, ad.getFields().getFields());
    Assert.assertEquals("The timeunit should be DAYS.", TimeUnit.DAYS, ad.getTimeBucket().getTimeUnit());
  }

  @Test
  public void equalsAndHashCodeTest()
  {
    DimensionsDescriptor ddA = new DimensionsDescriptor(new CustomTimeBucket(TimeBucket.MINUTE, 5L),
        new Fields(Sets.newHashSet("a", "b")));

    DimensionsDescriptor ddB = new DimensionsDescriptor(new CustomTimeBucket(TimeBucket.MINUTE, 5L),
        new Fields(Sets.newHashSet("a", "b")));

    Assert.assertTrue(ddB.equals(ddA));
  }
}
