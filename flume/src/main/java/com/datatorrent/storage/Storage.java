package com.datatorrent.storage;

public interface Storage
{
  
  
  /**
   * This stores the bytes and returns the unique identifier to retrieve these bytes
   * @param bytes
   * @return
   */
  public long store(byte[] bytes);
  
  /**
   * This returns the bytes identified by the identifier
   * @return
   */
  public RetrievalObject retrieve(long identifier);
  
  /**
   * This returns the the bytes 
   * @return
   */
  public RetrievalObject retrieveNext();
  
  /**
   * This is used to clean up of the bytes identified by identifier 
   */
  public boolean clean(long identifier);
  
  public boolean flush();

}
