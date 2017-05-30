/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.batch;

import org.apache.apex.api.operator.ControlTuple;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

@Evolving
public interface FileControlTuple extends ControlTuple
{
  @Evolving
  public static class StartFileControlTuple implements FileControlTuple
  {
    private String filename;
    private boolean failedFile;

    /**
     * Sets the filename for start file control tuple
     *
     * @param filename
     */
    public void setFilename(String filename)
    {
      this.filename = filename;
    }

    /**
     * Returns the filename
     *
     * @return
     */
    public String getFilename()
    {
      return filename;
    }

    /**
     * When set to true, the file had failed earlier. The tuples received after
     * this StartFileControlTuple may not be from start of this file. When set
     * to false, tuples received will be from beginning from file
     *
     * @param failedFile
     */
    public void setFailedFile(boolean failedFile)
    {
      this.failedFile = failedFile;
    }

    /**
     * If true, the file had failed earlier. The tuples received after this
     * StartFileControlTuple may not be from start of this file. If false,
     * tuples received will be from beginning from file
     *
     * @return failedFile
     */
    public boolean isFailedFile()
    {
      return failedFile;
    }

    public StartFileControlTuple()
    {
      //For Kryo
    }

    public StartFileControlTuple(String filename, boolean failedFile)
    {
      this.filename = filename;
      this.failedFile = failedFile;
    }

    @Override
    public DeliveryType getDeliveryType()
    {
      return DeliveryType.IMMEDIATE;
    }
  }

  @Evolving
  public static class EndFileControlTuple implements FileControlTuple
  {
    private String filename;
    private boolean failedFile;

    public EndFileControlTuple()
    {
      //For Kryo
    }

    public EndFileControlTuple(String filename, boolean failedFile)
    {
      this.filename = filename;
      this.failedFile = failedFile;
    }

    public void setFilename(String filename)
    {
      this.filename = filename;
    }

    public String getFilename()
    {
      return filename;
    }

    /**
     * Set to true to indicate that the file processing has failed midway. The
     * tuples till now may not be till end of the file. Set to false to indicate
     * that tuples till end of file are sent i.e. file processing was successful
     *
     * @param failedFile
     */
    public void setFailedFile(boolean failedFile)
    {
      this.failedFile = failedFile;
    }

    /**
     * If true, it indicates that the file processing has failed midway. The
     * tuples till now may not be till end of the file. If false, it implies
     * that tuples till end of file are sent, i.e. file processing was
     * successful
     *
     * @return failedFile
     */
    public boolean isFailedFile()
    {
      return failedFile;
    }

    @Override
    public DeliveryType getDeliveryType()
    {
      return DeliveryType.END_WINDOW;
    }
  }
}
