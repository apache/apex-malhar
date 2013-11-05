package com.datatorrent.lib.util;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.common.util.Slice;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.ParameterizedType;

/**
 * This codec is used for serializing the objects of class which are Kryo serializable.
 * It is needed when custom static partitioning is required.
 *
 * @param <T>
 *          Type of the object which gets serialized/deserialized using this  codec.
 *
 * @since 0.9.0
 */
public class KryoSerializableStreamCodec<T> implements StreamCodec<T>
{
  private final Kryo kryo;

  public KryoSerializableStreamCodec()
  {
    this.kryo = new Kryo();
    this.kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
    this.kryo.register((Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0]);
  }

  /**
   * Register classes of fields in the tuple with kryo.
   *
   * @param clazz class that is registered with Kryo which associates the class with an int ID
   */
  public void register(Class clazz)
  {
    this.kryo.register(clazz);
  }

  /**
   * Register classes with specified ids.
   *
   * @param clazz class that is registered with Kryo with associates the class with provided int ID
   * @param id    greater than 0
   */
  public void register(Class clazz, int id)
  {
    Preconditions.checkArgument(id > 0, "invalid id");
    this.kryo.register(clazz, id);
  }

  @Override
  public T fromByteArray(Slice fragment)
  {
    ByteArrayInputStream is = new ByteArrayInputStream(fragment.buffer, fragment.offset, fragment.length);
    Input input = new Input(is);
    return kryo.readObject(input, (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0]);
  }

  @Override
  public Slice toByteArray(T info)
  {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    Output output = new Output(os);
    kryo.writeObject(output, info);
    output.flush();
    return new Slice(os.toByteArray(), 0, os.toByteArray().length);
  }

  @Override
  public int getPartition(T t)
  {
    return t.hashCode();
  }
}
