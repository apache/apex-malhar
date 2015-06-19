/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.contrib.util;

import java.lang.reflect.Constructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TupleGenerator<T>
{
  private static final Logger logger = LoggerFactory.getLogger( TupleGenerator.class );
      
  private volatile long rowId = 0;
  private Constructor<T> constructor;
  
  private static Class<?>[] paramTypes = new Class<?>[]{ Long.class, long.class, Integer.class, int.class };
  
  public TupleGenerator( Class<T> tupleClass )
  {
    for( Class<?> paramType : paramTypes )
    {
      constructor = tryGetConstructor( tupleClass, paramType );
      if( constructor != null )
        break;
    }
    if( constructor == null )
    {
      logger.error( "Not found proper constructor." );
      throw new RuntimeException( "Not found proper constructor." );
    }
  }
  
  protected Constructor<T> tryGetConstructor( Class<T> tupleClass, Class<?> parameterType )
  {
    try
    {
      return tupleClass.getConstructor( parameterType );
    }
    catch( Exception e )
    {
      return null;
    }
  }
  
  public void reset()
  {
    rowId = 0;
  }
  
  public T getNextTuple()
  {
    if( constructor == null )
      throw new RuntimeException( "Not found proper constructor." );
    
    long curRowId = ++rowId;
    try
    {
      return constructor.newInstance( curRowId );
    }
    catch( Exception e)
    {
      logger.error( "newInstance failed.", e );
      return null;
    }
  }

}
