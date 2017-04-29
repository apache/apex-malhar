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
package org.apache.apex.examples.mrmonitor;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
   * This field stores the RM port information for hadoop 2.x / Task Manager server port for hadoop 1.X from where we
   * can get the job information
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
  private TaskObject metricObject;

  /**
   * This holds the number of windows occurred when the new data was retrieved for this job
   */
  private int retrials;

  /**
   * The scheduler is used to store the job status every 1 minute
   */
  private transient ScheduledExecutorService statusScheduler;

  /**
   * This stores the progress of the map tasks
   */
  Queue<String> mapStatusHistory;

  /**
   * This stores the progress of the reduce tasks
   */
  Queue<String> reduceStatusHistory;

  /**
   * This stores the history of the physical memory usage
   */
  Queue<String> physicalMemoryStatusHistory;

  /**
   * This stores the history of the virtual memory usage
   */
  Queue<String> virtualMemoryStatusHistory;

  /**
   * This stores the history of the cpu
   */
  Queue<String> cpuStatusHistory;

  /**
   * The number of minutes for which the status history of map and reduce tasks is stored
   */
  private int statusHistoryCount = 60;

  /**
   * This field notifies if history status queues have changed over time
   */
  private boolean changedHistoryStatus;

  public MRStatusObject()
  {
    retrials = 0;
    modified = true;
    mapJsonObject = new ConcurrentHashMap<String, TaskObject>();
    reduceJsonObject = new ConcurrentHashMap<String, TaskObject>();
    mapStatusHistory = new LinkedList<String>();
    reduceStatusHistory = new LinkedList<String>();
    physicalMemoryStatusHistory = new LinkedList<String>();
    virtualMemoryStatusHistory = new LinkedList<String>();
    cpuStatusHistory = new LinkedList<String>();
    statusScheduler = Executors.newScheduledThreadPool(1);
    statusScheduler.scheduleAtFixedRate(new Runnable()
    {
      @Override
      public void run()
      {
        if (jsonObject != null) {
          changedHistoryStatus = true;
          if (mapStatusHistory.size() > statusHistoryCount) {
            mapStatusHistory.poll();
            reduceStatusHistory.poll();
            physicalMemoryStatusHistory.poll();
            virtualMemoryStatusHistory.poll();
            cpuStatusHistory.poll();
          }
          if (hadoopVersion == 2) {
            try {
              mapStatusHistory.add(jsonObject.getJSONObject("job").getString("mapProgress"));
              reduceStatusHistory.add(jsonObject.getJSONObject("job").getString("reduceProgress"));
              if (metricObject.getJson() != null) {
                JSONArray arr = metricObject.getJson().getJSONObject("jobCounters").getJSONArray("counterGroup");
                int length = arr.length();
                for (int i = 0; i < length; i++) {
                  if (arr.getJSONObject(i).get("counterGroupName").equals("org.apache.hadoop.mapreduce.TaskCounter")) {
                    JSONArray counters = arr.getJSONObject(i).getJSONArray("counter");
                    for (int j = 0; j < counters.length(); j++) {
                      JSONObject counterObj = counters.getJSONObject(j);
                      if (counterObj.get("name").equals("PHYSICAL_MEMORY_BYTES")) {
                        physicalMemoryStatusHistory.add(counterObj.getString("totalCounterValue"));
                      } else if (counterObj.get("name").equals("VIRTUAL_MEMORY_BYTES")) {
                        virtualMemoryStatusHistory.add(counterObj.getString("totalCounterValue"));
                      } else if (counterObj.get("name").equals("CPU_MILLISECONDS")) {
                        cpuStatusHistory.add(counterObj.getString("totalCounterValue"));
                      }
                    }
                    break;
                  }
                }
              }
            } catch (JSONException e) {
              logger.error("error setting status history {}", e.getMessage());
            }
          } else {
            try {
              mapStatusHistory.add(jsonObject.getJSONObject("mapTaskSummary").getString("progressPercentage"));
              reduceStatusHistory.add(jsonObject.getJSONObject("reduceTaskSummary").getString("progressPercentage"));
              JSONArray arr = jsonObject.getJSONArray("jobCounterGroupsInfo");
              int length = arr.length();
              for (int i = 0; i < length; i++) {
                if (arr.getJSONObject(i).get("groupName").equals("Map-Reduce Framework")) {
                  JSONArray counters = arr.getJSONObject(i).getJSONArray("jobCountersInfo");
                  for (int j = 0; j < counters.length(); j++) {
                    JSONObject counterObj = counters.getJSONObject(j);
                    if (counterObj.get("name").equals("Physical memory (bytes) snapshot")) {
                      physicalMemoryStatusHistory.add(counterObj.getString("totalValue"));
                    } else if (counterObj.get("name").equals("Virtual memory (bytes) snapshot")) {
                      virtualMemoryStatusHistory.add(counterObj.getString("totalValue"));
                    } else if (counterObj.get("name").equals("CPU time spent (ms)")) {
                      cpuStatusHistory.add(counterObj.getString("totalValue"));
                    }
                  }
                  break;
                }
              }
            } catch (JSONException e) {
              logger.error("error setting status history {}", e.getMessage());
            }
          }
        }
      }
    }, 0, 1, TimeUnit.MINUTES);
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

  public boolean isChangedHistoryStatus()
  {
    return changedHistoryStatus;
  }

  public void setChangedHistoryStatus(boolean changedHistoryStatus)
  {
    this.changedHistoryStatus = changedHistoryStatus;
  }

  @Override
  public boolean equals(Object that)
  {
    if (this == that) {
      return true;
    }
    if (!(that instanceof MRStatusObject)) {
      return false;
    }
    if (this.hashCode() == that.hashCode()) {
      return true;
    }
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

  public int getRetrials()
  {
    return retrials;
  }

  public void setRetrials(int retrials)
  {
    this.retrials = retrials;
  }

  public TaskObject getMetricObject()
  {
    return metricObject;
  }

  public void setMetricObject(TaskObject metricObject)
  {
    this.metricObject = metricObject;
  }

  public int getStatusHistoryCount()
  {
    return statusHistoryCount;
  }

  public void setStatusHistoryCount(int statusHistoryCount)
  {
    this.statusHistoryCount = statusHistoryCount;
  }

  public Queue<String> getMapStatusHistory()
  {
    return mapStatusHistory;
  }

  public Queue<String> getReduceStatusHistory()
  {
    return reduceStatusHistory;
  }

  public Queue<String> getPhysicalMemeoryStatusHistory()
  {
    return physicalMemoryStatusHistory;
  }

  public Queue<String> getVirtualMemoryStatusHistory()
  {
    return virtualMemoryStatusHistory;
  }

  public Queue<String> getCpuStatusHistory()
  {
    return cpuStatusHistory;
  }

  public static class TaskObject
  {
    /**
     * This field stores the task information as json
     */
    private JSONObject json;
    /**
     * This field tells if the object was modified
     */
    private boolean modified;

    public TaskObject(JSONObject json)
    {
      modified = true;
      this.json = json;
    }

    /**
     * This returns the task information as json
     *
     * @return
     */
    public JSONObject getJson()
    {
      return json;
    }

    /**
     * This stores the task information as json
     *
     * @param json
     */
    public void setJson(JSONObject json)
    {
      this.json = json;
    }

    /**
     * This returns if the json object has been modified
     *
     * @return
     */
    public boolean isModified()
    {
      return modified;
    }

    /**
     * This sets if the json object is modified
     *
     * @param modified
     */
    public void setModified(boolean modified)
    {
      this.modified = modified;
    }

    /**
     * This returns the string format of the json object
     *
     * @return
     */
    public String getJsonString()
    {
      return json.toString();
    }
  }

  private static Logger logger = LoggerFactory.getLogger(MRStatusObject.class);

}
