package org.apache.apex.malhar.lib.utils.serde;

/**
 * Created by tfarkas on 8/28/16.
 */
public interface Serializer<OBJ, SER>
{
  /**
   * Serialized the given object.
   * @param object The object to serialize.
   * @return The serialized representation of the object.
   */
  SER serialize(OBJ object);
}
