package com.datatorrent.contrib.enrichment;

// This class is needed for Bean Enrichment Operator testing
public class EmployeeOrder {
  public int OID;
  public int ID;
  public double amount;
  public String NAME;
  public int AGE;
  public String ADDRESS;
  public double SALARY;

  public int getOID()
  {
    return OID;
  }

  public void setOID(int OID)
  {
    this.OID = OID;
  }

  public int getID()
  {
    return ID;
  }

  public void setID(int ID)
  {
    this.ID = ID;
  }

  public int getAGE()
  {
    return AGE;
  }

  public void setAGE(int AGE)
  {
    this.AGE = AGE;
  }

  public String getNAME()
  {
    return NAME;
  }

  public void setNAME(String NAME)
  {
    this.NAME = NAME;
  }

  public double getAmount()
  {
    return amount;
  }

  public void setAmount(double amount)
  {
    this.amount = amount;
  }

  public String getADDRESS()
  {
    return ADDRESS;
  }

  public void setADDRESS(String ADDRESS)
  {
    this.ADDRESS = ADDRESS;
  }

  public double getSALARY()
  {
    return SALARY;
  }

  public void setSALARY(double SALARY)
  {
    this.SALARY = SALARY;
  }

  @Override public String toString()
  {
    return "{" +
        "OID=" + OID +
        ", ID=" + ID +
        ", amount=" + amount +
        ", NAME='" + NAME + '\'' +
        ", AGE=" + AGE +
        ", ADDRESS='" + ADDRESS.trim() + '\'' +
        ", SALARY=" + SALARY +
        '}';
  }
}
