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

package com.datatorrent.contrib.hdht.export;

import com.datatorrent.contrib.hdht.HDHTCodec;
import com.datatorrent.contrib.hdht.HDSFileExporter;

import javax.validation.constraints.NotNull;

/**
 * Base class for performing file exports to various formats.
 */
public abstract class AbstractHDHTFileExporter<EVENT> implements HDSFileExporter {

  @NotNull
  private String basePath;
  private transient HDHTCodec<EVENT> codec;

  public String getBasePath()
  {
    return basePath;
  }
  public void setBasePath(String path)
  {
    this.basePath = path;
  }


  AbstractHDHTFileExporter(HDHTCodec<EVENT> codec) {
    this.codec = codec;
  }

  public HDHTCodec<EVENT> getCodec() {
    return codec;
  }
  public void setCodec(HDHTCodec<EVENT> codec) {
    this.codec = codec;
  }

}
