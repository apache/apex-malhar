package com.example.myapexapp;

public class POJOEnriched
{
  private String phone;
  private String imei;
  private String imsi;
  private int circleId;
  private String circleName;

  public String getPhone()
  {
    return phone;
  }

  public void setPhone(String phone)
  {
    this.phone = phone;
  }

  public String getImei()
  {
    return imei;
  }

  public void setImei(String imei)
  {
    this.imei = imei;
  }

  public String getImsi()
  {
    return imsi;
  }

  public void setImsi(String imsi)
  {
    this.imsi = imsi;
  }

  public int getCircleId()
  {
    return circleId;
  }

  public void setCircleId(int circleId)
  {
    this.circleId = circleId;
  }

  public String getCircleName()
  {
    return circleName;
  }

  public void setCircleName(String circleName)
  {
    this.circleName = circleName;
  }

  @Override public String toString()
  {
    return "POJOEnriched{" +
        "phone='" + phone + '\'' +
        ", imei='" + imei + '\'' +
        ", imsi='" + imsi + '\'' +
        ", circleId=" + circleId +
        ", circleName='" + circleName + '\'' +
        '}';
  }
}
