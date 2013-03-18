/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
* Serializer for Java serializable objects in  Kryo framework. <p><br>
 *
 * <br>
 * The serializer implements a Kryo Serializer to serialize Java serializable objects.
 * It uses Java serialization to actually convert the object into byte array before
 * writing it out using Kryo. Similarly it uses java deserialization to create the
 * object from byte array read from Kryo. This can be used for objects
 * that cannot be automatically serialized by Kryo such as objects that do not
 * have a default constructor or contain a object member or in a member sub-hierarchy
 * that does not have a default constructor.
 *
 * The serialization scheme uses a simple mechanism to notify the deserialzer about the
 * length of the serialized object by serializing the length and including it before the serialized
 * object data. The deserializer can use this information to determine how many bytes to read
 * for deserialization of the object. <br>
 * <br>
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class KryoJavaSerializer<T> extends Serializer<T>
{

  @Override
  public void write(Kryo kryo, Output output, T t)
  {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(t);
      oos.close();
      byte[] ba = baos.toByteArray();
      output.writeInt(ba.length);
      output.write(ba);
      //System.out.println("enc len " + ba.length);
    }
    catch (Exception ex) {
      throw new RuntimeException(ex.getMessage());
    }
  }

  @Override
  public T read(Kryo kryo, Input input, Class<T> type)
  {
    T t = null;
    try {
      int length = input.readInt();
      byte[] ba = new byte[length];
      input.readBytes(ba);
      //System.out.println("des len " + ba.length);
      ByteArrayInputStream bais = new ByteArrayInputStream(ba);
      ObjectInputStream ois = new ObjectInputStream(bais);
      t = (T)ois.readObject();
    }
    catch (Exception ex) {
      ex.printStackTrace();
      throw new RuntimeException(ex.getMessage());
    }
    return t;
  }

}
