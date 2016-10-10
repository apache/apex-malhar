package com.example.transform;

public class CustomerInfo
{
  private int customerId;
  private String name;
  private int age;
  private String address;

  public int getCustomerId()
  {
    return customerId;
  }

  public void setCustomerId(int customerId)
  {
    this.customerId = customerId;
  }

  public String getName()
  {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  public int getAge()
  {
    return age;
  }

  public void setAge(int age)
  {
    this.age = age;
  }

  public String getAddress()
  {
    return address;
  }

  public void setAddress(String address)
  {
    this.address = address;
  }

  @Override
  public String toString()
  {
    return "CustomerInfo{" +
      "customerId=" + customerId +
      ", name='" + name + '\'' +
      ", age=" + age +
      ", address='" + address + '\'' +
      '}';
  }
}
