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
package org.apache.apex.malhar.lib.fs.s3;

import com.amazonaws.services.s3.AmazonS3;

import static org.apache.apex.malhar.lib.fs.s3.S3OutputModuleMockTest.client;

public class S3OutputTestModule extends S3OutputModule
{

  public static class S3InitiateFileUploadTest extends S3InitiateFileUploadOperator
  {
    @Override
    protected AmazonS3 createClient()
    {
      return client;
    }
  }

  public static class S3BlockUploadTest extends S3BlockUploadOperator
  {
    @Override
    protected AmazonS3 createClient()
    {
      return client;
    }
  }

  public static class S3FileMergerTest extends S3FileMerger
  {
    @Override
    protected AmazonS3 createClient()
    {
      return client;
    }
  }

  @Override
  protected S3InitiateFileUploadOperator createS3InitiateUpload()
  {
    return new S3InitiateFileUploadTest();
  }

  @Override
  protected S3BlockUploadOperator createS3BlockUpload()
  {
    return new S3BlockUploadTest();
  }

  @Override
  protected S3FileMerger createS3FileMerger()
  {
    return new S3FileMergerTest();
  }
}
