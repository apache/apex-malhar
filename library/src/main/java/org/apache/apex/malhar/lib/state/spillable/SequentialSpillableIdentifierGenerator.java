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

import java.util.Set;

import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * This is an id generator that generates single byte ids for Spillable datastructures.
 *
 * @since 3.5.0
 */
@InterfaceStability.Evolving
public class SequentialSpillableIdentifierGenerator implements SpillableIdentifierGenerator
{
  private boolean nextCalled = false;
  private boolean done = false;
  private byte currentIdentifier = 0;

  private Set<Byte> registeredIdentifier = Sets.newHashSet();

  @Override
  public byte[] next()
  {
    Preconditions.checkState(!done);

    nextCalled = true;

    byte nextIndentifier = currentIdentifier;
    seek();

    return new byte[]{nextIndentifier};
  }

  @Override
  public void register(byte[] identifierArray)
  {
    Preconditions.checkState(!nextCalled);
    Preconditions.checkState(!done);
    Preconditions.checkArgument(identifierArray.length == 1);

    byte identifier = identifierArray[0];

    Preconditions.checkState(identifier >= currentIdentifier &&
        !registeredIdentifier.contains(identifier));

    registeredIdentifier.add(identifier);

    if (currentIdentifier == identifier) {
      seek();
    }
  }

  private void seek()
  {
    if (currentIdentifier == Byte.MAX_VALUE) {
      done = true;
    } else {
      do {
        currentIdentifier++;
      } while (registeredIdentifier.contains(currentIdentifier) && currentIdentifier < Byte.MAX_VALUE);

      done = currentIdentifier == Byte.MAX_VALUE && registeredIdentifier.contains(currentIdentifier);
    }
  }
}
