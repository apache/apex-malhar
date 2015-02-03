/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.hive;

import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.*;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * An optional stream codec for uniform distribution of tuples on upstream operator.
 * This is for users who want a single dt partition to handle tuples for the same hive partition.
 */
public class HiveStreamCodec<T> extends KryoSerializableStreamCodec<T> implements Externalizable
{
  private static final long serialVersionUID = 201412121604L;

  protected FSRollingOutputOperator<T> rollingOperator;

  @Override
  public void writeExternal(ObjectOutput out) throws IOException
  {

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    ObjectOutputStream obj = new ObjectOutputStream(os);
    Output output = new Output(obj);
    kryo.writeClassAndObject(output, rollingOperator);
    byte[] outBytes = output.toBytes();
    out.writeInt(outBytes.length);
    out.write(outBytes, 0, outBytes.length);
    out.flush();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
  {
    int size = in.readInt();
    byte[] data = new byte[size];
    in.readFully(data);
    Input input = new Input(data);
    input.setBuffer(data);
    rollingOperator = (FSRollingOutputOperator)kryo.readClassAndObject(input);
  }


  @Override
  public int getPartition(T o)
  {
    return rollingOperator.getHivePartition(o).hashCode();
  }


}
