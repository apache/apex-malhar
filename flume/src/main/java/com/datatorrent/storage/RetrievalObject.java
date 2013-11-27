package com.datatorrent.storage;

public class RetrievalObject
{
  private long token;
  private byte[] data;
  
  public long getToken()
  {
    return token;
  }
  public void setToken(long token)
  {
    this.token = token;
  }
  public byte[] getData()
  {
    return data;
  }
  public void setData(byte[] data)
  {
    this.data = data;
  }

  @Override
  public String toString(){
    return new String(data);
  }
}
