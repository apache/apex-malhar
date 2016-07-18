/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */

package com.datatorrent.tutorial.filter;

public class TransactionPOJO
{

  private long trasactionId;
  private double amount;
  private long accountNumber;
  private String type;

  public long getTrasactionId()
  {
    return trasactionId;
  }

  public void setTrasactionId(long trasactionId)
  {
    this.trasactionId = trasactionId;
  }

  public double getAmount()
  {
    return amount;
  }

  public void setAmount(double amount)
  {
    this.amount = amount;
  }

  public long getAccountNumber()
  {
    return accountNumber;
  }

  public void setAccountNumber(long accountNumber)
  {
    this.accountNumber = accountNumber;
  }
  
  public String getType()
  {
    return type;
  }

  public void setType(String type)
  {
    this.type = type;
  }

  @Override
  public String toString()
  {
    return "TransactionPOJO [trasactionId=" + trasactionId + ", amount=" + amount + ", accountNumber=" + accountNumber
        + ", type=" + type + "]";
  }
  
  
}
