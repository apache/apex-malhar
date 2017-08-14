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
package org.apache.apex.malhar.lib.appdata.gpo;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.appdata.schemas.FieldsDescriptor;
import org.apache.apex.malhar.lib.appdata.schemas.Type;
import org.apache.commons.lang3.mutable.MutableInt;

import com.google.common.collect.Maps;

public class SerdeFieldsDescriptorTest
{
  @Test
  public void simpleTest()
  {
    Map<String, Type> fieldToType = Maps.newHashMap();

    fieldToType.put("a", Type.INTEGER);
    fieldToType.put("b", Type.CHAR);

    FieldsDescriptor fd = new FieldsDescriptor(fieldToType);

    byte[] bytes = SerdeFieldsDescriptor.INSTANCE.serializeObject(fd);
    FieldsDescriptor newfd = (FieldsDescriptor)SerdeFieldsDescriptor.INSTANCE.deserializeObject(bytes,
        new MutableInt(0));

    Assert.assertEquals(fd, newfd);
  }
}
