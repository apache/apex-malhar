/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.goldengate.utils;

import java.io.Serializable;

import com.goldengate.atg.datasource.GGTranID;

/**
 * A serializable version of Golden Gate's GGTranID object.
 */
public class _GGTranID implements Serializable
{
  private static final long serialVersionUID = -3881608675748255214L;
  private String position;
  private long RBA;
  private long seqNo;
  private String string;

  /**
   * Loads the data from the given GGTranID object.
   * @param ggid The GGTranID object to load data from.
   */
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
