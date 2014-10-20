package com.datatorrent.contrib.goldengate.lib;

import java.io.Serializable;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;


import com.goldengate.atg.datasource.DsOperation;
import com.goldengate.atg.datasource.DsTransaction;

public class _DsTransaction implements Serializable
{

  /**
   * 
   */
  private static final long serialVersionUID = 1157110970672330162L;

  private Date readTime;
  
  private int size;
  
  private int totalOps;
  
  private _GGTranID ggTranID;
  
  private List<_DsOperation> ops;


//  public void readFromDsTransaction(DsTransaction dt, DsMetaData dsMetadata){
//    this.setDsMetadata(dsMetadata);
//  }
  
  public void readFromDsTransaction(DsTransaction dt){
    readTime = dt.getReadTime();
    size = dt.getSize();
    totalOps = dt.getTotalOps();
    ggTranID = new _GGTranID();
    ggTranID.readFromGGTranID(dt.getTranID());
    ops = new LinkedList<_DsOperation>();
    for (DsOperation dsOperation : dt) {
      _DsOperation dso = new _DsOperation();
      dso.readFromDsOperation(dsOperation);
      ops.add(dso);
    }
  }



  public Date getReadTime()
  {
    return readTime;
  }


  public void setReadTime(Date readTime)
  {
    this.readTime = readTime;
  }


  public int getSize()
  {
    return size;
  }


  public void setSize(int size)
  {
    this.size = size;
  }


  public int getTotalOps()
  {
    return totalOps;
  }


  public void setTotalOps(int totalOps)
  {
    this.totalOps = totalOps;
  }


  public _GGTranID getGgTranID()
  {
    return ggTranID;
  }


  public void setGgTranID(_GGTranID ggTranID)
  {
    this.ggTranID = ggTranID;
  }


  public static long getSerialversionuid()
  {
    return serialVersionUID;
  }
  
  public void setOps(List<_DsOperation> ops)
  {
    this.ops = ops;
  }
  
  
  public List<_DsOperation> getOps()
  {
    return ops;
  }

  @Override
  public String toString()
  {
    return "_DsTransaction [readTime=" + readTime + ", size=" + size + ", totalOps=" + totalOps + ", ggTranID=" + ggTranID + ", dsMetadata=" + ", ops=" + ops + "]";
  }
  
  

}
