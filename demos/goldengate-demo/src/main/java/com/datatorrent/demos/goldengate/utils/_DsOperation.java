package com.datatorrent.demos.goldengate.utils;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import com.goldengate.atg.datasource.DsColumn;
import com.goldengate.atg.datasource.DsOperation;
import com.goldengate.atg.datasource.DsOperation.OpType;
import com.goldengate.atg.datasource.TxState;

public class _DsOperation implements Serializable
{
  /**
   * 
   */
  private static final long serialVersionUID = -4424245782506321708L;

  private List<_DsColumn> cols;
  
  private int numCols = 0;
  
  private OpType operationType;
  
  private String position;
  
  private long positionRba;

  private long positionSeqno;

  private String sqlType;

  private _TableName tableName;

  private TxState txState;
  
  public void readFromDsOperation(DsOperation dsOp){
    numCols = dsOp.getNumColumns();
    operationType = dsOp.getOperationType();
    position = dsOp.getPosition();
    positionRba = dsOp.getPositionRba();
    positionSeqno = dsOp.getPositionSeqno();
    sqlType = dsOp.getSqlType();
    tableName = new _TableName();
    tableName.readFromTableName(dsOp.getTableName());
    txState = dsOp.getTxState();
    

    cols = new LinkedList<_DsColumn>();
    for (DsColumn dsColumn : dsOp) {
      _DsColumn _col = new _DsColumn();
      _col.readFromDsColumn(dsColumn);
      cols.add(_col);
    }
  }

  public List<_DsColumn> getCols()
  {
    return cols;
  }

  public void setCols(List<_DsColumn> cols)
  {
    this.cols = cols;
  }

  public int getNumCols()
  {
    return numCols;
  }

  public void setNumCols(int numCols)
  {
    this.numCols = numCols;
  }

  public OpType getOperationType()
  {
    return operationType;
  }

  public void setOperationType(OpType operationType)
  {
    this.operationType = operationType;
  }

  public String getPosition()
  {
    return position;
  }

  public void setPosition(String position)
  {
    this.position = position;
  }

  public long getPositionRba()
  {
    return positionRba;
  }

  public void setPositionRba(long positionRba)
  {
    this.positionRba = positionRba;
  }

  public long getPositionSeqno()
  {
    return positionSeqno;
  }

  public void setPositionSeqno(long positionSeqno)
  {
    this.positionSeqno = positionSeqno;
  }


  public String getSqlType()
  {
    return sqlType;
  }

  public void setSqlType(String sqlType)
  {
    this.sqlType = sqlType;
  }

  public _TableName getTableName()
  {
    return tableName;
  }

  public void setTableName(_TableName tableName)
  {
    this.tableName = tableName;
  }

  public TxState getTxState()
  {
    return txState;
  }

  public void setTxState(TxState txState)
  {
    this.txState = txState;
  }

  @Override
  public String toString()
  {
    return "_DsOperation [cols=" + cols + ", numCols=" + numCols + ", operationType=" + operationType + ", position=" + position + ", positionRba=" + positionRba + ", positionSeqno=" + positionSeqno + ", sqlType=" + sqlType + ", tableName=" + tableName + ", txState=" + txState + "]";
  }
  
  

}
