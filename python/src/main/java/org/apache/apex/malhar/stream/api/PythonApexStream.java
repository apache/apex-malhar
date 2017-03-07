package org.apache.apex.malhar.stream.api;



/**
 * Created by vikram on 7/6/17.
 */
public interface PythonApexStream<T> extends ApexStream<T>
{

  /**
   * Add custom serialized Python Function along with options
   * @param serializedFunction stores Serialized Function data
   * @return new stream of type T
   */
  <STREAM extends PythonApexStream<T>> STREAM map(byte[] serializedFunction, Option... opts);

  /**
   * Add custom serialized Python Function along with options
   * @param serializedFunction stores Serialized Function data
   * @return new stream of type T
   */
  <STREAM extends PythonApexStream<T>> STREAM flatMap(byte[] serializedFunction, Option... opts);

  /**
   * Add custom serialized Python Function along with options
   * @param serializedFunction stores Serialized Function data
   * @return new stream of type T
   */
  <STREAM extends PythonApexStream<T>> STREAM filter(byte[] serializedFunction, Option... opts);

}
