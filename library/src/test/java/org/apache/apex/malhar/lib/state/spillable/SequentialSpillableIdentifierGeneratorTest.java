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
package org.apache.apex.malhar.lib.state.spillable;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.util.TestUtils;

public class SequentialSpillableIdentifierGeneratorTest
{
  @Test
  public void dontAllowRegistrationAfterNextCallTest()
  {
    SequentialSpillableIdentifierGenerator gen = new SequentialSpillableIdentifierGenerator();

    gen.next();

    boolean exception = false;

    try {
      gen.register(TestUtils.getByte(1));
    } catch (Exception e) {
      exception = true;
    }

    Assert.assertTrue(exception);
  }

  @Test
  public void simpleSequentialIdGenerationTest()
  {
    SequentialSpillableIdentifierGenerator gen = new SequentialSpillableIdentifierGenerator();

    for (int index = 0; index < (((int)Byte.MAX_VALUE) + 1); index++) {
      byte[] id = gen.next();

      checkId(index, id);
    }

    boolean threwException = false;

    try {
      gen.next();
    } catch (Exception e) {
      threwException = true;
    }

    Assert.assertTrue(threwException);
  }

  @Test
  public void registerFirst()
  {
    SequentialSpillableIdentifierGenerator gen = new SequentialSpillableIdentifierGenerator();
    gen.register(TestUtils.getByte(0));

    byte[] id = gen.next();

    Assert.assertArrayEquals(TestUtils.getByte(1), id);
  }

  @Test
  public void registerLast()
  {
    SequentialSpillableIdentifierGenerator gen = new SequentialSpillableIdentifierGenerator();
    gen.register(TestUtils.getByte(Byte.MAX_VALUE));

    for (int index = 0; index <= (((int)Byte.MAX_VALUE) - 1); index++) {
      byte[] id = gen.next();

      checkId(index, id);
    }

    boolean threwException = false;

    try {
      gen.next();
    } catch (Exception e) {
      threwException = true;
    }

    Assert.assertTrue(threwException);
  }

  @Test
  public void intermingledRegistered()
  {
    SequentialSpillableIdentifierGenerator gen = new SequentialSpillableIdentifierGenerator();

    gen.register(TestUtils.getByte(1));
    gen.register(TestUtils.getByte(2));
    gen.register(TestUtils.getByte(5));
    gen.register(TestUtils.getByte(7));

    checkId(0, gen.next());
    checkId(3, gen.next());
    checkId(4, gen.next());
    checkId(6, gen.next());
    checkId(8, gen.next());
    checkId(9, gen.next());
  }

  private void checkId(int val, byte[] id)
  {
    Assert.assertEquals(1, id.length);
    Assert.assertEquals(val, id[0]);
  }
}
