/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.demos.mrmonitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.codehaus.jettison.json.JSONObject;

/**
 * <p>
 * MRStatusObject class.
 * </p>
 *
 * @since 0.3.4
 */
public class MRStatusObject
{
  private String command;
  private String uri;
  private String jobId;
  private String apiVersion;
  private int hadoopVersion;
  private String appId;
  private int rmPort;
  private int historyServerPort;
  private JSONObject jsonObject;
  private boolean modified;
  private Map<String, TaskObject> mapJsonObject;
  private Map<String, TaskObject> reduceJsonObject;

  public MRStatusObject()
  {
    modified = true;
    mapJsonObject = new ConcurrentHashMap<String, TaskObject>();
    reduceJsonObject = new ConcurrentHashMap<String, TaskObject>();
  }

  public Map<String, TaskObject> getMapJsonObject()
  {
    return mapJsonObject;
  }

  public void setMapJsonObject(Map<String, TaskObject> mapJsonObject)
  {
    this.mapJsonObject = mapJsonObject;
  }

  public Map<String, TaskObject> getReduceJsonObject()
  {
    return reduceJsonObject;
  }

  public void setReduceJsonObject(Map<String, TaskObject> reduceJsonObject)
  {
    this.reduceJsonObject = reduceJsonObject;
  }

  public String getUri()
  {
    return uri;
  }

  public void setUri(String uri)
  {
    this.uri = uri;
  }

  public String getJobId()
  {
    return jobId;
  }

  public void setJobId(String jobId)
  {
    this.jobId = jobId;
  }

  public String getApiVersion()
  {
    return apiVersion;
  }

  public void setApiVersion(String apiVersion)
  {
    this.apiVersion = apiVersion;
  }

  public int getHadoopVersion()
  {
    return hadoopVersion;
  }

  public void setHadoopVersion(int hadoopVersion)
  {
    this.hadoopVersion = hadoopVersion;
  }

  public String getAppId()
  {
    return appId;
  }

  public void setAppId(String appId)
  {
    this.appId = appId;
  }

  public int getRmPort()
  {
    return rmPort;
  }

  public void setRmPort(int rmPort)
  {
    this.rmPort = rmPort;
  }

  public int getHistoryServerPort()
  {
    return historyServerPort;
  }

  public void setHistoryServerPort(int historyServerPort)
  {
    this.historyServerPort = historyServerPort;
  }

  public JSONObject getJsonObject()
  {
    return jsonObject;
  }

  public void setJsonObject(JSONObject jsonObject)
  {
    this.jsonObject = jsonObject;
  }

  @Override
  public boolean equals(Object that)
  {
    if (this == that)
      return true;
    if (!(that instanceof MRStatusObject))
      return false;
    if (this.hashCode() == that.hashCode())
      return true;
    return false;
  }

  @Override
  public int hashCode()
  {
    return (uri + jobId + apiVersion + String.valueOf(hadoopVersion)).hashCode();

  }

  public String getCommand()
  {
    return command;
  }

  public void setCommand(String command)
  {
    this.command = command;
  }

  public boolean isModified()
  {
    return modified;
  }

  public void setModified(boolean modified)
  {
    this.modified = modified;
  }
  
  public static class TaskObject{
    private JSONObject json;
    private boolean modified;
    
    public TaskObject(JSONObject json){
      modified = true;
      this.json = json;
    }

    public JSONObject getJson()
    {
      return json;
    }

    public void setJson(JSONObject json)
    {
      this.json = json;
    }

    public boolean isModified()
    {
      return modified;
    }

    public void setModified(boolean modified)
    {
      this.modified = modified;
    }
    
    public String getJsonString(){
      return json.toString();
    }
    
    
  }
}
