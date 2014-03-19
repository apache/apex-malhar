package com.datatorrent.lib.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;

import com.datatorrent.api.StreamCodec;

import com.datatorrent.common.util.Slice;

/**
 * This codec is used for serializing the objects of class which are Kryo serializable.
 * It is needed when custom static partitioning is required.
 *
 * @param <T> Type of the object which gets serialized/deserialized using this  codec.
 * @since 0.9.0
 */
public class KryoSerializableStreamCodec<T> implements StreamCodec<T>
{
  protected final transient Kryo kryo;

  public KryoSerializableStreamCodec()
  {
    this.kryo = new Kryo();
    this.kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
  }

  /**
   * Registers a class with kryo. If the class of the tuple and its fields are registered then kryo serialization is more efficient.
   *
   * @param clazz class to register with Kryo.
   */
  public void register(Class<?> clazz)
  {
    this.kryo.register(clazz);
  }

  /**
   * Register a class with specified id.
   *
   * @param clazz class to register with Kryo.
   * @param id    int ID of the class.
   */
  public void register(Class<?> clazz, int id)
  {
    Preconditions.checkArgument(id > 0, "invalid id");
    this.kryo.register(clazz, id);
  }

  @Override
  public Object fromByteArray(Slice fragment)
  {
    ByteArrayInputStream is = new ByteArrayInputStream(fragment.buffer, fragment.offset, fragment.length);
    Input input = new Input(is);
    return kryo.readClassAndObject(input);
  }

  @Override
  public Slice toByteArray(T info)
  {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    Output output = new Output(os);
    kryo.writeClassAndObject(output, info);
    output.flush();
    return new Slice(os.toByteArray(), 0, os.toByteArray().length);
  }

  @Override
  public int getPartition(T t)
  {
    return t.hashCode();
  }
}
