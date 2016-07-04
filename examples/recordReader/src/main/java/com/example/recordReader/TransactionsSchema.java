/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */

package com.example.recordReader;

import java.util.Date;

public class TransactionsSchema
{
  private String customerName;
  private String customerPhone;
  private String customerEmail;
  private String city;
  private String country;
  private String uid;
  private String accountNumber;
  private long txId;
  private Date txDate;
  private double txAmount;

  public String getCustomerName()
  {
    return customerName;
  }

  public void setCustomerName(String customerName)
  {
    this.customerName = customerName;
  }

  public String getCustomerPhone()
  {
    return customerPhone;
  }

  public void setCustomerPhone(String customerPhone)
  {
    this.customerPhone = customerPhone;
  }

  public String getCustomerEmail()
  {
    return customerEmail;
  }

  public void setCustomerEmail(String customerEmail)
  {
    this.customerEmail = customerEmail;
  }

  public String getCity()
  {
    return city;
  }

  public void setCity(String city)
  {
    this.city = city;
  }

  public String getCountry()
  {
    return country;
  }

  public void setCountry(String country)
  {
    this.country = country;
  }

  public String getUid()
  {
    return uid;
  }

  public void setUid(String uid)
  {
    this.uid = uid;
  }

  public String getAccountNumber()
  {
    return accountNumber;
  }

  public void setAccountNumber(String accountNumber)
  {
    this.accountNumber = accountNumber;
  }

  public long getTxId()
  {
    return txId;
  }

  public void setTxId(long txId)
  {
    this.txId = txId;
  }

  public Date getTxDate()
  {
    return txDate;
  }

  public void setTxDate(Date txDate)
  {
    this.txDate = txDate;
  }

  public double getTxAmount()
  {
    return txAmount;
  }

  public void setTxAmount(double txAmount)
  {
    this.txAmount = txAmount;
  }
  
  @Override
  public String toString()
  {
    return "TransactionsSchema [customerName=" + customerName + ", customerPhone=" + customerPhone + ", customerEmail="
        + customerEmail + ", city=" + city + ", country=" + country + ", uid=" + uid + ", accountNumber="
        + accountNumber + ", txId=" + txId + ", txDate=" + txDate + ", txAmount=" + txAmount 
        + "]\n";
  }

  @Override
  public int hashCode()
  {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((txDate == null) ? 0 : txDate.hashCode());
    result = prime * result + (int)(txId ^ (txId >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    TransactionsSchema other = (TransactionsSchema)obj;
    if (txDate == null) {
      if (other.txDate != null) {
        return false;
      }
    } else if (!txDate.equals(other.txDate)) {
      return false;
    }
    if (txId != other.txId) {
      return false;
    }
    return true;
  }

}
