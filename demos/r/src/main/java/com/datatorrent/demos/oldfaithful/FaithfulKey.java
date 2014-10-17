package com.datatorrent.demos.oldfaithful;

public class FaithfulKey
{

  private static final long serialVersionUID = 201403251620L;

  private double eruptionDuration;
  private int waitingTime;

  public FaithfulKey()
  {
  }

  public double getEruptionDuration()
  {
    return eruptionDuration;
  }

  public void setEruptionDuration(double eruptionDuration)
  {
    this.eruptionDuration = eruptionDuration;
  }

  public int getWaitingTime()
  {
    return waitingTime;
  }

  public void setWaitingTime(int waitingTime)
  {
    this.waitingTime = waitingTime;
  }
}
