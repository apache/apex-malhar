package com.datatorrent.contrib.model;

import java.lang.reflect.Constructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TupleGenerator<T>
{
  private static final Logger logger = LoggerFactory.getLogger( TupleGenerator.class );
      
  private volatile long rowId = 1;
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
  
  public T getNextTuple()
  {
    if( constructor == null )
      throw new RuntimeException( "Not found proper constructor." );
    
    long curRowId = rowId++;
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
