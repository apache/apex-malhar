/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import com.esotericsoftware.kryo.DefaultSerializer;
import java.io.Serializable;

/**
 * KryoJavaContainer wraps a Java serializable object and sets up a Kryo Serializer.
 *
 * This class implements a simple wrapper for a Java serializable object that isn't directly
 * serializable in Kryo. This could be for reasons such as the object does not contain a
 * a default constructor or a member object in the sub-hierarchy of the object does not
 * contain a default constructor.<br>
 * <br>
 * The container provides a Kryo Serializable container for such an object. This can be used
 * when the object code cannot be modified to use the KryoJavaSerializer directly.<br>
 * <br>
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
@DefaultSerializer(KryoJavaSerializer.class)
public class KryoJavaContainer<T> implements Serializable
{

  private T t;

  public KryoJavaContainer(){
  }

  public KryoJavaContainer(T t) {
    this.t = t;
  }

  public void setMember(T t) {
    this.t = t;
  }

  public T getMember() {
    return t;
  }

  @Override
  public boolean equals(Object o) {
    boolean equal = false;
    if (o instanceof KryoJavaContainer) {
      KryoJavaContainer k = (KryoJavaContainer)o;
      equal = t.equals(k.getMember());
    }
    return equal;
  }

}
