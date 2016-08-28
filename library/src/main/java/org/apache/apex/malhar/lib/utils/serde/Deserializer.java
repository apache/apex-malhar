package org.apache.apex.malhar.lib.utils.serde;

import org.apache.commons.lang3.mutable.MutableInt;

/**
 * Created by tfarkas on 8/28/16.
 */
public interface Deserializer<OBJ, SER>
{
  /**
   * Deserializes the given serialized representation of an object.
   * @param object The serialized representation of an object.
   * @param offset An offset in the serialized representation of the object. After the
   * deserialize method completes the offset is updated, so that the offset points to
   * the remaining unprocessed portion of the serialized object. For example:<br/>
   * {@code
   * Object obj;
   * MutableInt mi;
   * someObj1 = deserialize(obj, mi);
   * someObj2 = deserialize(obj, mi);
   * }
   *
   * @return The deserialized object.
   */
  OBJ deserialize(SER object, MutableInt offset);

  /**
   * Deserializes the given serialized representation of an object.
   * @param object The serialized representation of an object.
   *
   * @return The deserialized object.
   */
  OBJ deserialize(SER object);
}
