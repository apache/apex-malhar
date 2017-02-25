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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.apex.examples.mrmonitor.MRStatusObject.TaskObject;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.IdleTimeHandler;

/**
 * <p>
 * MRJobStatusOperator class.
 * </p>
 *
 * @since 0.3.4
 */
public class MRJobStatusOperator implements Operator, IdleTimeHandler
{
  private static final Logger LOG = LoggerFactory.getLogger(MRJobStatusOperator.class);

  private static final String JOB_PREFIX = "job_";
  /**
   * This outputs the meta information of the job
   */
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
  /**
   * This outputs the map task information of the job
   */
  public final transient DefaultOutputPort<String> mapOutput = new DefaultOutputPort<String>();
  /**
   * This outputs the reduce task information of the job
   */
  public final transient DefaultOutputPort<String> reduceOutput = new DefaultOutputPort<String>();
  /**
   * This outputs the counter information of the job
   */
  public final transient DefaultOutputPort<String> counterOutput = new DefaultOutputPort<String>();
  /**
   * This is time in Ms before making new request for data
   */
  private transient int sleepTime = 100;
  /**
   * This is the number of consecutive windows of no change before the job is removed from map
   */
  private int maxRetrials = 10;
  /**
   * The number of minutes for which the status history of map and reduce tasks is stored
   */
  private int statusHistoryTime = 60;
  private Map<String, MRStatusObject> jobMap = new HashMap<String, MRStatusObject>();
  /**
   * This represents the maximum number of jobs the single instance of this operator is going to server at any time
   */
  private int maxJobs = Constants.MAX_NUMBER_OF_JOBS;
  private transient Iterator<MRStatusObject> iterator;

  /*
   * each input string is a map of the following format {"app_id":<>,"hadoop_version":<>,"api_version":<>,"command":<>,
   * "hostname":<>,"hs_port":<>,"rm_port":<>,"job_id":<>}
   */
  public final transient DefaultInputPort<MRStatusObject> input = new DefaultInputPort<MRStatusObject>()
  {
    @Override
    public void process(MRStatusObject mrStatusObj)
    {

      if (jobMap == null) {
        jobMap = new HashMap<String, MRStatusObject>();
      }

      if (jobMap.size() >= maxJobs) {
        return;
      }

      if ("delete".equalsIgnoreCase(mrStatusObj.getCommand())) {
        removeJob(mrStatusObj.getJobId());
        JSONObject outputJsonObject = new JSONObject();
        try {
          outputJsonObject.put("id", mrStatusObj.getJobId());
          outputJsonObject.put("removed", "true");
          output.emit(outputJsonObject.toString());
        } catch (JSONException e) {
          LOG.warn("Error creating JSON: {}", e.getMessage());
        }
        return;
      }
      if ("clear".equalsIgnoreCase(mrStatusObj.getCommand())) {
        clearMap();
        return;
      }

      if (jobMap.get(mrStatusObj.getJobId()) != null) {
        mrStatusObj = jobMap.get(mrStatusObj.getJobId());
      }
      if (mrStatusObj.getHadoopVersion() == 2) {
        getJsonForJob(mrStatusObj);
      } else if (mrStatusObj.getHadoopVersion() == 1) {
        getJsonForLegacyJob(mrStatusObj);
      }
      mrStatusObj.setStatusHistoryCount(statusHistoryTime);
      iterator = jobMap.values().iterator();
      emitHelper(mrStatusObj);
    }
  };

  public int getStatusHistoryTime()
  {
    return statusHistoryTime;
  }

  public void setStatusHistoryTime(int statusHistoryTime)
  {
    this.statusHistoryTime = statusHistoryTime;
    if (jobMap != null && jobMap.size() > 0) {
      for (Entry<String, MRStatusObject> entry : jobMap.entrySet()) {
        entry.getValue().setStatusHistoryCount(statusHistoryTime);
      }
    }

  }

  /**
   * This method gets the latest status of the job from the Resource Manager for jobs submitted on hadoop 2.x version
   *
   * @param statusObj
   */
  private void getJsonForJob(MRStatusObject statusObj)
  {

    String url = "http://" + statusObj.getUri() + ":" + statusObj.getRmPort() + "/proxy/application_" + statusObj.getAppId() + "/ws/v1/mapreduce/jobs/job_" + statusObj.getJobId();
    String responseBody = MRUtil.getJsonForURL(url);

    JSONObject jsonObj = MRUtil.getJsonObject(responseBody);

    if (jsonObj == null) {
      url = "http://" + statusObj.getUri() + ":" + statusObj.getHistoryServerPort() + "/ws/v1/history/mapreduce/jobs/job_" + statusObj.getJobId();
      responseBody = MRUtil.getJsonForURL(url);
      jsonObj = MRUtil.getJsonObject(responseBody);
    }

    if (jsonObj != null) {
      if (jobMap.get(statusObj.getJobId()) != null) {
        MRStatusObject tempObj = jobMap.get(statusObj.getJobId());
        if (tempObj.getJsonObject().toString().equals(jsonObj.toString())) {
          getJsonsForTasks(statusObj);
          getCounterInfoForJob(statusObj);
          return;
        }
      }
      statusObj.setModified(true);
      statusObj.setJsonObject(jsonObj);
      getCounterInfoForJob(statusObj);
      getJsonsForTasks(statusObj);
      jobMap.put(statusObj.getJobId(), statusObj);
    }
  }

  /**
   * This method is used to collect the metric information about the job
   *
   * @param statusObj
   */
  private void getCounterInfoForJob(MRStatusObject statusObj)
  {
    String url = "http://" + statusObj.getUri() + ":" + statusObj.getRmPort() + "/proxy/application_" + statusObj.getAppId() + "/ws/v1/mapreduce/jobs/job_" + statusObj.getJobId() + "/counters";
    String responseBody = MRUtil.getJsonForURL(url);
    JSONObject jsonObj = MRUtil.getJsonObject(responseBody);
    if (jsonObj == null) {
      url = "http://" + statusObj.getUri() + ":" + statusObj.getHistoryServerPort() + "/ws/v1/history/mapreduce/jobs/job_" + statusObj.getJobId() + "/counters";
      responseBody = MRUtil.getJsonForURL(url);
      jsonObj = MRUtil.getJsonObject(responseBody);
    }

    if (jsonObj != null) {
      if (statusObj.getMetricObject() == null) {
        statusObj.setMetricObject(new TaskObject(jsonObj));
      } else if (!statusObj.getMetricObject().getJsonString().equalsIgnoreCase(jsonObj.toString())) {
        statusObj.getMetricObject().setJson(jsonObj);
        statusObj.getMetricObject().setModified(true);
      }
    }
  }

  /**
   * This method gets the latest status of the tasks for a job from the Resource Manager for jobs submitted on hadoop
   * 2.x version
   *
   * @param statusObj
   */
  private void getJsonsForTasks(MRStatusObject statusObj)
  {
    String url = "http://" + statusObj.getUri() + ":" + statusObj.getRmPort() + "/proxy/application_" + statusObj.getAppId() + "/ws/v1/mapreduce/jobs/job_" + statusObj.getJobId() + "/tasks/";
    String responseBody = MRUtil.getJsonForURL(url);

    JSONObject jsonObj = MRUtil.getJsonObject(responseBody);
    if (jsonObj == null) {
      url = "http://" + statusObj.getUri() + ":" + statusObj.getHistoryServerPort() + "/ws/v1/history/mapreduce/jobs/job_" + statusObj.getJobId() + "/tasks/";
      responseBody = MRUtil.getJsonForURL(url);

      jsonObj = MRUtil.getJsonObject(responseBody);
    }

    if (jsonObj != null) {

      try {
        Map<String, TaskObject> mapTaskOject = statusObj.getMapJsonObject();
        Map<String, TaskObject> reduceTaskOject = statusObj.getReduceJsonObject();
        JSONArray taskJsonArray = jsonObj.getJSONObject("tasks").getJSONArray("task");

        for (int i = 0; i < taskJsonArray.length(); i++) {
          JSONObject taskObj = taskJsonArray.getJSONObject(i);
          if (Constants.REDUCE_TASK_TYPE.equalsIgnoreCase(taskObj.getString(Constants.TASK_TYPE))) {
            if (reduceTaskOject.get(taskObj.getString(Constants.TASK_ID)) != null) {
              TaskObject tempTaskObj = reduceTaskOject.get(taskObj.getString(Constants.TASK_ID));
              if (tempTaskObj.getJsonString().equals(taskObj.toString())) {
                continue;
              }
              tempTaskObj.setJson(taskObj);
              tempTaskObj.setModified(true);
              reduceTaskOject.put(taskObj.getString(Constants.TASK_ID), tempTaskObj);
              continue;
            }
            reduceTaskOject.put(taskObj.getString(Constants.TASK_ID), new TaskObject(taskObj));
          } else {
            if (mapTaskOject.get(taskObj.getString(Constants.TASK_ID)) != null) {
              TaskObject tempTaskObj = mapTaskOject.get(taskObj.getString(Constants.TASK_ID));
              if (tempTaskObj.getJsonString().equals(taskObj.toString())) {
                continue;
              }
              tempTaskObj.setJson(taskObj);
              tempTaskObj.setModified(true);
              mapTaskOject.put(taskObj.getString(Constants.TASK_ID), tempTaskObj);
              continue;
            }
            mapTaskOject.put(taskObj.getString(Constants.TASK_ID), new TaskObject(taskObj));
          }
        }
        statusObj.setMapJsonObject(mapTaskOject);
        statusObj.setReduceJsonObject(reduceTaskOject);
      } catch (Exception e) {
        LOG.info("exception: {}", e.getMessage());
      }
    }

  }

  /**
   * This method gets the latest status of the job from the Task Manager for jobs submitted on hadoop 1.x version
   *
   * @param statusObj
   */
  private void getJsonForLegacyJob(MRStatusObject statusObj)
  {

    String url = "http://" + statusObj.getUri() + ":" + statusObj.getRmPort() + "/jobdetails.jsp?format=json&jobid=job_" + statusObj.getJobId();
    String responseBody = MRUtil.getJsonForURL(url);

    JSONObject jsonObj = MRUtil.getJsonObject(responseBody);
    if (jsonObj == null) {
      return;
    }

    if (jobMap.get(statusObj.getJobId()) != null) {
      MRStatusObject tempObj = jobMap.get(statusObj.getJobId());
      if (tempObj.getJsonObject().toString().equals(jsonObj.toString())) {
        getJsonsForLegacyTasks(statusObj, "map");
        getJsonsForLegacyTasks(statusObj, "reduce");
        // output.emit(jsonObj.toString());
        // removeJob(statusObj.getJobId());
        return;
      }
    }

    // output.emit(jsonObj.toString());
    statusObj.setModified(true);
    statusObj.setJsonObject(jsonObj);
    getJsonsForLegacyTasks(statusObj, "map");
    getJsonsForLegacyTasks(statusObj, "reduce");
    jobMap.put(statusObj.getJobId(), statusObj);

  }

  /**
   * This method gets the latest status of the tasks for a job from the Task Manager for jobs submitted on hadoop 1.x
   * version
   *
   * @param statusObj
   * @param type
   */
  private void getJsonsForLegacyTasks(MRStatusObject statusObj, String type)
  {
    try {
      JSONObject jobJson = statusObj.getJsonObject();
      int totalTasks = ((JSONObject)((JSONObject)jobJson.get(type + "TaskSummary")).get("taskStats")).getInt("numTotalTasks");
      Map<String, TaskObject> taskMap;
      if (type.equalsIgnoreCase("map")) {
        taskMap = statusObj.getMapJsonObject();
      } else {
        taskMap = statusObj.getReduceJsonObject();
      }

      int totalPagenums = (totalTasks / Constants.MAX_TASKS) + 1;
      String baseUrl = "http://" + statusObj.getUri() + ":" + statusObj.getRmPort() + "/jobtasks.jsp?type=" + type + "&format=json&jobid=job_" + statusObj.getJobId() + "&pagenum=";

      for (int pagenum = 1; pagenum <= totalPagenums; pagenum++) {

        String url = baseUrl + pagenum;
        String responseBody = MRUtil.getJsonForURL(url);

        JSONObject jsonObj = MRUtil.getJsonObject(responseBody);
        if (jsonObj == null) {
          return;
        }

        JSONArray taskJsonArray = jsonObj.getJSONArray("tasksInfo");

        for (int i = 0; i < taskJsonArray.length(); i++) {
          JSONObject taskObj = taskJsonArray.getJSONObject(i);
          {
            if (taskMap.get(taskObj.getString(Constants.LEAGACY_TASK_ID)) != null) {
              TaskObject tempReduceObj = taskMap.get(taskObj.getString(Constants.LEAGACY_TASK_ID));
              if (tempReduceObj.getJsonString().equals(taskObj.toString())) {
                // tempReduceObj.setModified(false);
                // taskMap.put(taskObj.getString(Constants.TASK_ID), tempReduceObj);
                continue;
              }
              tempReduceObj.setJson(taskObj);
              tempReduceObj.setModified(true);
              taskMap.put(taskObj.getString(Constants.TASK_ID), tempReduceObj);
              continue;

            }
            taskMap.put(taskObj.getString(Constants.LEAGACY_TASK_ID), new TaskObject(taskObj));
          }
        }
      }

      if (type.equalsIgnoreCase("map")) {
        statusObj.setMapJsonObject(taskMap);
      } else {
        statusObj.setReduceJsonObject(taskMap);
      }
    } catch (Exception e) {
      LOG.info(e.getMessage());
    }

  }

  @Override
  public void handleIdleTime()
  {
    try {
      Thread.sleep(sleepTime);//
    } catch (InterruptedException ie) {
      // If this thread was intrrupted by nother thread
    }
    if (!iterator.hasNext()) {
      iterator = jobMap.values().iterator();
    }

    if (iterator.hasNext()) {
      MRStatusObject obj = iterator.next();
      if (obj.getHadoopVersion() == 2) {
        getJsonForJob(obj);
      } else if (obj.getHadoopVersion() == 1) {
        getJsonForLegacyJob(obj);
      }
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
    iterator = jobMap.values().iterator();
    sleepTime = context.getValue(OperatorContext.SPIN_MILLIS);
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void beginWindow(long arg0)
  {
  }

  private void emitHelper(MRStatusObject obj)
  {
    try {
      obj.setModified(false);
      output.emit(obj.getJsonObject().toString());
      JSONObject outputJsonObject = new JSONObject();

      outputJsonObject.put("id", JOB_PREFIX + obj.getJobId());
      outputJsonObject.put("mapHistory", new JSONArray(obj.getMapStatusHistory()));
      outputJsonObject.put("reduceHistory", new JSONArray(obj.getReduceStatusHistory()));
      outputJsonObject.put("physicalMemoryHistory", new JSONArray(obj.getPhysicalMemeoryStatusHistory()));
      outputJsonObject.put("virtualMemoryHistory", new JSONArray(obj.getVirtualMemoryStatusHistory()));
      outputJsonObject.put("cpuHistory", new JSONArray(obj.getCpuStatusHistory()));
      output.emit(outputJsonObject.toString());
      obj.setChangedHistoryStatus(false);

      outputJsonObject = new JSONObject();
      outputJsonObject.put("id", JOB_PREFIX + obj.getJobId());
      JSONArray arr = new JSONArray();

      for (Map.Entry<String, TaskObject> mapEntry : obj.getMapJsonObject().entrySet()) {
        TaskObject json = mapEntry.getValue();
        json.setModified(false);
        arr.put(json.getJson());
      }

      outputJsonObject.put("tasks", arr);
      mapOutput.emit(outputJsonObject.toString());

      outputJsonObject = new JSONObject();
      outputJsonObject.put("id", JOB_PREFIX + obj.getJobId());
      arr = new JSONArray();

      for (Map.Entry<String, TaskObject> mapEntry : obj.getReduceJsonObject().entrySet()) {
        TaskObject json = mapEntry.getValue();
        json.setModified(false);
        arr.put(json.getJson());
      }

      outputJsonObject.put("tasks", arr);
      reduceOutput.emit(outputJsonObject.toString());
      obj.setRetrials(0);
    } catch (Exception e) {
      LOG.warn("error creating json {}", e.getMessage());
    }

  }

  @Override
  public void endWindow()
  {
    List<String> delList = new ArrayList<String>();
    try {
      for (Map.Entry<String, MRStatusObject> entry : jobMap.entrySet()) {
        MRStatusObject obj = entry.getValue();

        JSONObject outputJsonObject = new JSONObject();
        outputJsonObject.put("id", JOB_PREFIX + obj.getJobId());

        boolean modified = false;

        if (obj.isModified()) {
          modified = true;
          obj.setModified(false);
          output.emit(obj.getJsonObject().toString());
          if (obj.isChangedHistoryStatus()) {
            outputJsonObject.put("mapHistory", new JSONArray(obj.getMapStatusHistory()));
            outputJsonObject.put("reduceHistory", new JSONArray(obj.getReduceStatusHistory()));
            outputJsonObject.put("physicalMemoryHistory", new JSONArray(obj.getPhysicalMemeoryStatusHistory()));
            outputJsonObject.put("virtualMemoryHistory", new JSONArray(obj.getVirtualMemoryStatusHistory()));
            outputJsonObject.put("cpuHistory", new JSONArray(obj.getCpuStatusHistory()));
            output.emit(outputJsonObject.toString());
            obj.setChangedHistoryStatus(false);
          }
        }
        outputJsonObject = new JSONObject();
        outputJsonObject.put("id", JOB_PREFIX + obj.getJobId());
        JSONArray arr = new JSONArray();

        for (Map.Entry<String, TaskObject> mapEntry : obj.getMapJsonObject().entrySet()) {
          TaskObject json = mapEntry.getValue();
          if (json.isModified()) {
            modified = true;
            json.setModified(false);
            arr.put(json.getJson());
          }
        }

        if (arr.length() > 0) {
          outputJsonObject.put("tasks", arr);
          mapOutput.emit(outputJsonObject.toString());
        }

        outputJsonObject = new JSONObject();
        outputJsonObject.put("id", JOB_PREFIX + obj.getJobId());
        arr = new JSONArray();

        for (Map.Entry<String, TaskObject> mapEntry : obj.getReduceJsonObject().entrySet()) {
          TaskObject json = mapEntry.getValue();
          if (json.isModified()) {
            modified = true;
            json.setModified(false);
            arr.put(json.getJson());
          }
        }
        if (arr.length() > 0) {
          outputJsonObject.put("tasks", arr);
          reduceOutput.emit(outputJsonObject.toString());
        }

        if (obj.getMetricObject() != null && obj.getMetricObject().isModified()) {
          modified = true;
          obj.getMetricObject().setModified(false);
          counterOutput.emit(obj.getMetricObject().getJsonString());
        }

        if (!modified) {
          if (obj.getRetrials() >= maxRetrials) {
            delList.add(obj.getJobId());
          } else {
            obj.setRetrials(obj.getRetrials() + 1);
          }
        } else {
          obj.setRetrials(0);
        }
      }
    } catch (Exception ex) {
      LOG.warn("error creating json {}", ex.getMessage());
    }

    if (!delList.isEmpty()) {
      Iterator<String> itr = delList.iterator();
      while (itr.hasNext()) {
        removeJob(itr.next());
      }
    }

  }

  /**
   * This method removes the job from the map
   *
   * @param jobId
   */
  public void removeJob(String jobId)
  {
    if (jobMap != null) {
      jobMap.remove(jobId);
      iterator = jobMap.values().iterator();
    }
  }

  /**
   * This method clears the job map
   */
  public void clearMap()
  {
    if (jobMap != null) {
      jobMap.clear();
      iterator = jobMap.values().iterator();
    }
  }

  /**
   * This returns the maximum number of jobs the single instance of this operator is going to server at any time
   *
   * @return
   */
  public int getMaxJobs()
  {
    return maxJobs;
  }

  /**
   * This sets the maximum number of jobs the single instance of this operator is going to server at any time
   *
   * @param maxJobs
   */
  public void setMaxJobs(int maxJobs)
  {
    this.maxJobs = maxJobs;
  }

  /**
   * This sets the number of consecutive windows of no change before the job is removed from map
   *
   * @return
   */
  public int getMaxRetrials()
  {
    return maxRetrials;
  }

  /**
   * This returns the number of consecutive windows of no change before the job is removed from map
   *
   * @param maxRetrials
   */
  public void setMaxRetrials(int maxRetrials)
  {
    this.maxRetrials = maxRetrials;
  }

}
