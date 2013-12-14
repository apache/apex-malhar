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
  /**
   * This stores the Resource Manager/ Task Manager's host information
   */
  private String uri;
  /**
   * This field stores the job id
   */
  private String jobId;
  /**
   * This field stores the api version of the rest apis
   */
  private String apiVersion;
  /**
   * This field stores the hadoop version 1 for 1.x and 2 for 2.x
   */
  private int hadoopVersion;
  /**
   * This field stores the app id for the hadoop 2.x
   */
  private String appId;
  /**
   * This field stores the RM port information for hadoop 2.x / Task Manager server port for hadoop 1.X  from where we can get the job information
   */
  private int rmPort;
  /**
   * This field stores the history server information for hadoop 2.x from where we can get the job information
   */
  private int historyServerPort;
  /**
   * This field stores the job information as json object
   */
  private JSONObject jsonObject;
  /**
   * This field tells if the object has been modified 
   */
  private boolean modified;
  /**
   * This stores the mapping of map task ids to the TaskObject
   */
  private Map<String, TaskObject> mapJsonObject;
  /**
   * This stores the mapping of reduce task ids to the TaskObject
   */
  private Map<String, TaskObject> reduceJsonObject;
  /**
   * This holds the information about the various metrics like MAP_OUTPUT_RECORDS etc for this job 
   */
  private TaskObject counterObject;
  
  /**
   * This holds the number of windows occurred when the new data was retrieved for this job
   */
  private int retrials;

  public MRStatusObject()
  {
    retrials = 0;
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
    /**
     * This field stores the task information as json 
     */
    private JSONObject json;
    /**
     * This field tells if the object was modified
     */
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

    /**
     *  
     * @param modified
     */
    public void setModified(boolean modified)
    {
      this.modified = modified;
    }
    
    /**
     * This returns the string format of the json object 
     * @return
     */
    public String getJsonString(){
      return json.toString();
    }
  }
  
  public int getRetrials()
  {
    return retrials;
  }

  public void setRetrials(int retrials)
  {
    this.retrials = retrials;
  }

  public TaskObject getCounterObject()
  {
    return counterObject;
  }

  public void setCounterObject(TaskObject counterObject)
  {
    this.counterObject = counterObject;
  }

}
