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
package org.apache.apex.malhar.python.base.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import jep.NDArray;

/***
 * A handy Kryo serializer class that can be used to serialize and deserialize a JEP NDArray instance. It is
 *  recommended that {@link NDimensionalArray} be used in lieu of this class. This is because NDArray is highly specific
 *   JEP data structure and will not give flexibility if the python engines are changed in the future.
 */
public class NDArrayKryoSerializer extends Serializer<NDArray>
{
  private static final short TRUE_AS_SHORTINT = 1;

  private static final short FALSE_AS_SHORTINT = 0;

  @Override
  public void setGenerics(Kryo kryo, Class[] generics)
  {
    super.setGenerics(kryo, generics);
  }

  @Override
  public void write(Kryo kryo, Output output, NDArray ndArray)
  {

    Object dataVal = ndArray.getData();
    if (dataVal == null) {
      return;
    }
    // NDArray throws an exception in constructor if not an array. So below value will never be null
    Class classNameForArrayType = dataVal.getClass().getComponentType(); // null if it is not an array
    int[] dimensions = ndArray.getDimensions();
    boolean signedFlag = ndArray.isUnsigned();
    int arraySizeInSingleDimension = 1;
    for (int aDimensionSize : dimensions) {
      arraySizeInSingleDimension = arraySizeInSingleDimension * aDimensionSize;
    }
    // write the single dimension length
    output.writeInt(arraySizeInSingleDimension);
    // write the dimension of the dimensions int array
    output.writeInt(dimensions.length);
    // next we write the dimensions array itself
    output.writeInts(dimensions);

    // next write the unsigned flag
    output.writeBoolean(signedFlag);

    // write the data type of the N-dimensional Array
    if (classNameForArrayType != null) {
      output.writeString(classNameForArrayType.getCanonicalName());
    } else {
      output.writeString(null);
    }

    // write the array contents
    if (dataVal != null) {
      switch (classNameForArrayType.getCanonicalName()) {
        case "float":
          output.writeFloats((float[])dataVal);
          break;
        case "int":
          output.writeInts((int[])dataVal);
          break;
        case "double":
          output.writeDoubles((double[])dataVal);
          break;
        case "long":
          output.writeLongs((long[])dataVal);
          break;
        case "short":
          output.writeShorts((short[])dataVal);
          break;
        case "byte":
          output.writeBytes((byte[])dataVal);
          break;
        case "boolean":
          boolean[] originalBoolArray = (boolean[])dataVal;
          short[] convertedBoolArray = new short[originalBoolArray.length];
          for (int i = 0; i < originalBoolArray.length; i++) {
            if (originalBoolArray[i]) {
              convertedBoolArray[i] = TRUE_AS_SHORTINT;
            } else {
              convertedBoolArray[i] = FALSE_AS_SHORTINT;
            }
          }
          output.writeShorts(convertedBoolArray);
          break;
        default:
          throw new RuntimeException("Unsupported NDArray type serialization object");
      }
    }
  }

  @Override
  public NDArray read(Kryo kryo, Input input, Class<NDArray> aClass)
  {
    int singleDimensionArrayLength = input.readInt();
    int lengthOfDimensionsArray = input.readInt();
    int[] dimensions = input.readInts(lengthOfDimensionsArray);
    boolean signedFlag = input.readBoolean();

    String dataType = input.readString();
    if ( dataType == null) {
      return null;
    }
    switch (dataType) {
      case "float":
        NDArray<float[]> floatNDArray = new NDArray<>(
            input.readFloats(singleDimensionArrayLength),signedFlag,dimensions);
        return floatNDArray;
      case "int":
        NDArray<int[]> intNDArray = new NDArray<>(
            input.readInts(singleDimensionArrayLength),signedFlag,dimensions);
        return intNDArray;
      case "double":
        NDArray<double[]> doubleNDArray = new NDArray<>(
            input.readDoubles(singleDimensionArrayLength),signedFlag,dimensions);
        return doubleNDArray;
      case "long":
        NDArray<long[]> longNDArray = new NDArray<>(
            input.readLongs(singleDimensionArrayLength),signedFlag,dimensions);
        return longNDArray;
      case "short":
        NDArray<short[]> shortNDArray = new NDArray<>(
            input.readShorts(singleDimensionArrayLength),signedFlag,dimensions);
        return shortNDArray;
      case "byte":
        NDArray<byte[]> byteNDArray = new NDArray<>(
            input.readBytes(singleDimensionArrayLength),signedFlag,dimensions);
        return byteNDArray;
      case "boolean":
        short[] shortsArray = input.readShorts(singleDimensionArrayLength);
        boolean[] boolsArray = new boolean[shortsArray.length];
        for (int i = 0; i < shortsArray.length; i++) {
          if (TRUE_AS_SHORTINT == shortsArray[i]) {
            boolsArray[i] = true;
          } else {
            boolsArray[i] = false;
          }
        }
        NDArray<boolean[]> booleanNDArray = new NDArray<>(boolsArray,signedFlag,dimensions);
        return booleanNDArray;
      default:
        return null;
    }
  }
}
