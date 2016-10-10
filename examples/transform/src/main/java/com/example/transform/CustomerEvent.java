package com.example.transform;

import java.util.Date;

public class CustomerEvent
{
  private int customerId;
  private String firstName;
  private String lastName;
  private Date dateOfBirth;
  private String address;

  public int getCustomerId()
  {
    return customerId;
  }

  public void setCustomerId(int customerId)
  {
    this.customerId = customerId;
  }

  public String getFirstName()
  {
    return firstName;
  }

  public void setFirstName(String firstName)
  {
    this.firstName = firstName;
  }

  public String getLastName()
  {
    return lastName;
  }

  public void setLastName(String lastName)
  {
    this.lastName = lastName;
  }

  public Date getDateOfBirth()
  {
    return dateOfBirth;
  }

  public void setDateOfBirth(Date dateOfBirth)
  {
    this.dateOfBirth = dateOfBirth;
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
    return "CustomerEvent{" +
      "customerId=" + customerId +
      ", firstName='" + firstName + '\'' +
      ", lastName='" + lastName + '\'' +
      ", dateOfBirth=" + dateOfBirth +
      ", address='" + address + '\'' +
      '}';
  }
}
