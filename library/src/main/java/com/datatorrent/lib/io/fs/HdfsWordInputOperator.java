/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.io.fs;


//import java.io.DataInputStream;
import java.io.EOFException;
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;


import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.DTThrowable;

/**
 * Input operator to read words of specified size from HDFS file 
 * Reads words of specified size from the file in HDFS 
 * Reads as many words as possible in each dag window.
 *
 * @since 0.9.4
 */
public class HdfsWordInputOperator extends AbstractHDFSInputOperator
{

  @OutputPortFieldAnnotation(name = "HDFSOutput")
  public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<byte[]>();

  private transient FSDataInputStream dis = null;
  private byte[] tupleBuffer = new byte[64];
  private int tupleSize = 64;
  private transient int count;
  private boolean firstTime;

  @Override
  public void activate(OperatorContext ctx)
  {
    super.activate(ctx);
    String file = getFilePath();
    if (file != null) {
      dis = openFile(file); 
    }
  }
  
  /**
   * Sets the size of the word to be read from the file. 
   *  
   * @param size the tupleSize to set
   */
  public void setTupleSize(int size)
  {
    tupleSize = size;
    tupleBuffer = new byte[tupleSize];
  }

  /**
   * @return the tupleSize
   */
  public int getTupleSize()
  {
    return tupleSize;
  }

  @Override
  public void emitTuples(FSDataInputStream stream)
  {
    final int tupleSizeLocalCopy = tupleSize;
    try {
      if (firstTime) {
        for (int i = count--; i-- > 0;) {
          tupleBuffer =  new byte[tupleSizeLocalCopy];
          dis.readFully(0, tupleBuffer, 0, tupleSizeLocalCopy);
          output.emit(tupleBuffer); 
        }
        firstTime = false;
      }else {
        tupleBuffer =  new byte[tupleSizeLocalCopy];
        dis.readFully(0, tupleBuffer, 0, tupleSizeLocalCopy);
        output.emit(tupleBuffer); 
        count++;
      }
    } 
    catch(EOFException e)
    {
      super.seek(dis,0);
    }catch (IOException e) {
     DTThrowable.rethrow(e);
    }
  }
  
  @Override
  public void beginWindow(long windowId)
  {
    firstTime = true;
  }

  @Override
  public void endWindow()
  {
  }

}
