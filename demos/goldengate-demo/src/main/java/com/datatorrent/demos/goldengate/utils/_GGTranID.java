package com.datatorrent.demos.goldengate.utils;

import java.io.Serializable;

import com.goldengate.atg.datasource.GGTranID;

public class _GGTranID implements Serializable
{

  /**
   * 
   */
  private static final long serialVersionUID = -3881608675748255214L;
  private String position;
  private long RBA;
  private long seqNo;
  private String string;

  public void readFromGGTranID(GGTranID ggid)
  {
    position = ggid.getPosition();
    RBA = ggid.getRBA();

    seqNo = ggid.getSeqNo();
    string = ggid.getString();
  }

  public String getPosition()
  {
    return position;
  }

  public void setPosition(String position)
  {
    this.position = position;
  }

  public long getRBA()
  {
    return RBA;
  }

  public void setRBA(long rBA)
  {
    RBA = rBA;
  }

  public long getSeqNo()
  {
    return seqNo;
  }

  public void setSeqNo(long seqNo)
  {
    this.seqNo = seqNo;
  }

  public String getString()
  {
    return string;
  }

  public void setString(String string)
  {
    this.string = string;
  }

  @Override
  public String toString()
  {
    return "_GGTranID [position=" + position + ", RBA=" + RBA + ", seqNo=" + seqNo + ", string=" + string + "]";
  }

}
