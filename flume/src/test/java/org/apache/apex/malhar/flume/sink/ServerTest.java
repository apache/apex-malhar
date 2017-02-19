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
package org.apache.apex.malhar.flume.sink;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ServerTest
{
  byte[] array;

  public ServerTest()
  {
    array = new byte[1024];
  }

  @Test
  public void testInt()
  {
    Server.writeInt(array, 0, Integer.MAX_VALUE);
    Assert.assertEquals("Max Integer", Integer.MAX_VALUE, Server.readInt(array, 0));

    Server.writeInt(array, 0, Integer.MIN_VALUE);
    Assert.assertEquals("Min Integer", Integer.MIN_VALUE, Server.readInt(array, 0));

    Server.writeInt(array, 0, 0);
    Assert.assertEquals("Zero Integer", 0, Server.readInt(array, 0));

    Random rand = new Random();
    for (int i = 0; i < 128; i++) {
      int n = rand.nextInt();
      if (rand.nextBoolean()) {
        n = -n;
      }
      Server.writeInt(array, 0, n);
      Assert.assertEquals("Random Integer", n, Server.readInt(array, 0));
    }
  }

  @Test
  public void testLong()
  {
    Server.writeLong(array, 0, Integer.MAX_VALUE);
    Assert.assertEquals("Max Integer", Integer.MAX_VALUE, Server.readLong(array, 0));

    Server.writeLong(array, 0, Integer.MIN_VALUE);
    Assert.assertEquals("Min Integer", Integer.MIN_VALUE, Server.readLong(array, 0));

    Server.writeLong(array, 0, 0);
    Assert.assertEquals("Zero Integer", 0L, Server.readLong(array, 0));

    Server.writeLong(array, 0, Long.MAX_VALUE);
    Assert.assertEquals("Max Long", Long.MAX_VALUE, Server.readLong(array, 0));

    Server.writeLong(array, 0, Long.MIN_VALUE);
    Assert.assertEquals("Min Long", Long.MIN_VALUE, Server.readLong(array, 0));

    Server.writeLong(array, 0, 0L);
    Assert.assertEquals("Zero Long", 0L, Server.readLong(array, 0));

    Random rand = new Random();
    for (int i = 0; i < 128; i++) {
      long n = rand.nextLong();
      if (rand.nextBoolean()) {
        n = -n;
      }
      Server.writeLong(array, 0, n);
      Assert.assertEquals("Random Long", n, Server.readLong(array, 0));
    }
  }

}
