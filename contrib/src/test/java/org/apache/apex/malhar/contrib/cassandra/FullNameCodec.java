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
package org.apache.apex.malhar.contrib.cassandra;

import java.nio.ByteBuffer;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class FullNameCodec extends TypeCodec<FullName>
{

  private final TypeCodec<UDTValue> innerCodec;

  private final UserType userType;

  public FullNameCodec(TypeCodec<UDTValue> innerCodec, Class<FullName> javaType)
  {
    super(innerCodec.getCqlType(), javaType);
    this.innerCodec = innerCodec;
    this.userType = (UserType)innerCodec.getCqlType();
  }

  @Override
  public ByteBuffer serialize(FullName value, ProtocolVersion protocolVersion) throws InvalidTypeException
  {
    return innerCodec.serialize(toUDTValue(value), protocolVersion);
  }

  @Override
  public FullName deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException
  {
    return toFullName(innerCodec.deserialize(bytes, protocolVersion));
  }

  @Override
  public FullName parse(String value) throws InvalidTypeException
  {
    return value == null || value.isEmpty() ? null : toFullName(innerCodec.parse(value));
  }

  @Override
  public String format(FullName value) throws InvalidTypeException
  {
    return value == null ? null : innerCodec.format(toUDTValue(value));
  }

  protected FullName toFullName(UDTValue value)
  {
    return value == null ? null : new FullName(
      value.getString("firstname"),
      value.getString("lastname"));
  }

  protected UDTValue toUDTValue(FullName value)
  {
    return value == null ? null : userType.newValue()
      .setString("lastname", value.getLastname())
      .setString("firstname", value.getFirstname());
  }
}
