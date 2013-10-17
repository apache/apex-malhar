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
package com.datatorrent.contrib.machinedata;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.*;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.contrib.machinedata.data.MachineKey;
import com.datatorrent.contrib.machinedata.operator.CalculatorOperator;
import com.datatorrent.contrib.machinedata.operator.MachineInfoAveragingOperator;
import com.datatorrent.contrib.machinedata.operator.MachineInfoAveragingPrerequisitesOperator;
import com.datatorrent.contrib.redis.RedisOutputOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.SmtpOutputOperator;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Resource monitor application.</p>
 *
 * @since 0.3.5
 */
public class Application implements StreamingApplication {

    private static final Logger LOG = LoggerFactory.getLogger(Application.class);
    protected int streamingWindowSizeMilliSeconds = 1000; // 1 second
    //    protected int fileReadingWindowSizeMilliSeconds = 3000; // 3 second
    protected int tupleGeneratorWindowSizeMilliSeconds = 1000; // 1 second
    protected int appWindowCountMinute = 10;   // 10 seconds
    protected int compareAlertWindow = 15; // 1/4 minute
    private boolean isWebsocket = false;

    public RandomInformationTupleGenerator getRandomInformationTupleGenerator(String name, DAG dag) {
        RandomInformationTupleGenerator oper = dag.addOperator(name, RandomInformationTupleGenerator.class);
        dag.getOperatorMeta(name).getAttributes().attr(Context.OperatorContext.APPLICATION_WINDOW_COUNT).set(appWindowCountMinute);
        return oper;
    }

    public MachineInfoBucketOperator getMachineInfoBucketOperator(String name, DAG dag) {
        return dag.addOperator(name, MachineInfoBucketOperator.class);
    }


    public MachineInfoAveragingPrerequisitesOperator getMachineInfoAveragingPrerequisitesOperator(String name, DAG dag) {
        MachineInfoAveragingPrerequisitesOperator oper = dag.addOperator(name, MachineInfoAveragingPrerequisitesOperator.class);
        dag.getOperatorMeta(name).getAttributes().attr(Context.OperatorContext.APPLICATION_WINDOW_COUNT).set(appWindowCountMinute);
        return oper;
    }

    public MachineInfoAveragingOperator getMachineInfoAveragingOperator(String name, DAG dag) {
        MachineInfoAveragingOperator oper = dag.addOperator(name, MachineInfoAveragingOperator.class);
        dag.getOperatorMeta(name).getAttributes().attr(Context.OperatorContext.APPLICATION_WINDOW_COUNT).set(appWindowCountMinute);
        return oper;
    }

    public AlertGeneratorOperator getAlertGeneratorOperator(String name, DAG dag) {
        AlertGeneratorOperator oper = dag.addOperator(name, AlertGeneratorOperator.class);
        dag.getOperatorMeta(name).getAttributes().attr(Context.OperatorContext.APPLICATION_WINDOW_COUNT).set(appWindowCountMinute);
        return oper;
    }

    public RedisOutputOperator<MachineKey, Double> getRedisOutputOperator(String name, DAG dag, Configuration conf, int database) {
        RedisOutputOperator<MachineKey, Double> oper = dag.addOperator(name, new RedisOutputOperator<MachineKey, Double>());
        String host = conf.get("machinedata.redis.host", "localhost");
        int port = conf.getInt("machinedata.redis.port", 6379);
        oper.setHost(host);
        oper.setPort(port);
        oper.setDatabase(database);
        dag.getOperatorMeta(name).getAttributes().attr(Context.OperatorContext.APPLICATION_WINDOW_COUNT).set(appWindowCountMinute);
        return oper;
    }

    public InputPort<Object> getConsole(String name, DAG dag, String prefix) {
        ConsoleOutputOperator oper = dag.addOperator(name, ConsoleOutputOperator.class);
        oper.setStringFormat(prefix + ": %s");
        return oper.input;
    }

    public ConsoleOutputOperator getConsoleOperator(String name, DAG dag, String prefix) {
        ConsoleOutputOperator oper = dag.addOperator(name, ConsoleOutputOperator.class);
        oper.setStringFormat(prefix + ": %s");
        return oper;
    }

    public InputPort<Object> getFormattedConsole(String name, DAG dag, String format) {
        ConsoleOutputOperator oper = dag.addOperator(name, ConsoleOutputOperator.class);
        oper.setStringFormat(format);
        return oper.input;
    }

    public SmtpOutputOperator getSmtpOutputOperator(String name, DAG dag, Configuration conf) {
        SmtpOutputOperator mailOper = new SmtpOutputOperator();

        String from = conf.get("machinedata.smtp.from", "admin@datatorrent.com");
        String recipient = conf.get("machinedata.smtp.recipient", "atul@datatorrent.com");
        String subject = conf.get("machinedata.smtp.subject", "Alert!!!");
        String content = conf.get("machinedata.smtp.content", "{}");
        String host = conf.get("machinedata.smtp.host", "localhost");
        int port = conf.getInt("machinedata.smtp.port", 25);
//        String userName = conf.get("smtp.username", "");
//        String password = conf.get("smtp.password", "");
        boolean useSsl = conf.getBoolean("machinedata.smtp.ssl", false);

        mailOper.setFrom(from);
        mailOper.addRecipient(SmtpOutputOperator.RecipientType.TO, recipient);
        mailOper.setSubject(subject);
        mailOper.setContent(content);
        mailOper.setSmtpHost(host);
        mailOper.setSmtpPort(port);
//        mailOper.setSmtpUserName(userName);
//        mailOper.setSmtpPassword(password);
        mailOper.setUseSsl(useSsl);

        dag.addOperator(name, mailOper);

        return mailOper;

    }

    private void setDefaultInputPortQueueCapacity(DAG dag, InputPort inputPort) {
        dag.setInputPortAttribute(inputPort,PortContext.QUEUE_CAPACITY, 32*1024);
    }

    private void setDefaultOutputPortQueueCapacity(DAG dag, Operator.OutputPort outputPort){
        dag.setOutputPortAttribute(outputPort, PortContext.QUEUE_CAPACITY, 32*1024);
    }

    private MachineInfoAveragingPrerequisitesOperator addAverageCalculation(DAG dag, Configuration conf){
        MachineInfoAveragingPrerequisitesOperator prereqAverageOper = getMachineInfoAveragingPrerequisitesOperator("PrereqAverage", dag);
    	dag.setInputPortAttribute(prereqAverageOper.inputPort, PortContext.PARTITION_PARALLEL, true);

        setDefaultInputPortQueueCapacity(dag,prereqAverageOper.inputPort);
        setDefaultOutputPortQueueCapacity(dag,prereqAverageOper.outputPort);

        MachineInfoAveragingOperator averageOperator = getMachineInfoAveragingOperator("Average", dag);
        setDefaultInputPortQueueCapacity(dag,averageOperator.inputPort);
        setDefaultOutputPortQueueCapacity(dag,averageOperator.outputPort);

        RedisOutputOperator redisAvgOperator = getRedisOutputOperator("RedisAverageOutput", dag, conf, conf.getInt("machinedata.redis.db", 15));
        setDefaultInputPortQueueCapacity(dag,redisAvgOperator.inputInd);

        SmtpOutputOperator smtpOutputOperator = getSmtpOutputOperator("SmtpAvgOperator", dag, conf);

        dag.addStream("inter_avg", prereqAverageOper.outputPort, averageOperator.inputPort);
        dag.addStream("avg_output", averageOperator.outputPort, redisAvgOperator.inputInd);
        dag.addStream("avg_alert_mail", averageOperator.smtpAlert, smtpOutputOperator.input);

        return prereqAverageOper;

    }

    private CalculatorOperator addCalculator(DAG dag, Configuration conf){
        CalculatorOperator oper = dag.addOperator("Calculator",CalculatorOperator.class);
        dag.getOperatorMeta("Calculator").getAttributes().attr(Context.OperatorContext.APPLICATION_WINDOW_COUNT).set(appWindowCountMinute);
        //dag.setAttribute(oper, OperatorContext.INITIAL_PARTITION_COUNT,5);

        setDefaultInputPortQueueCapacity(dag,oper.dataPort);

        //Percentile
        setDefaultOutputPortQueueCapacity(dag,oper.percentileOutputPort);
        RedisOutputOperator redisPercentileOutput= getRedisOutputOperator("RedisPercentileOutput", dag,conf,conf.getInt("machinedata.percentile.redis.db",14));
        setDefaultInputPortQueueCapacity(dag,redisPercentileOutput.inputInd);
        dag.addStream("percentile_output", oper.percentileOutputPort,redisPercentileOutput.inputInd);

        //SD
        setDefaultOutputPortQueueCapacity(dag, oper.sdOutputPort);
        RedisOutputOperator redisSDOperator= getRedisOutputOperator("RedisSDOutput", dag,conf,conf.getInt("machinedata.sd.redis.db",13));
        setDefaultInputPortQueueCapacity(dag,redisSDOperator.inputInd);
        dag.addStream("sd_output", oper.sdOutputPort,redisSDOperator.inputInd);

        //Max
        setDefaultOutputPortQueueCapacity(dag, oper.maxOutputPort);
        RedisOutputOperator redisMaxOutput= getRedisOutputOperator("RedisMaxOutput", dag,conf,conf.getInt("machinedata.max.redis.db",12));
        setDefaultInputPortQueueCapacity(dag,redisMaxOutput.inputInd);
        dag.addStream("max_output", oper.maxOutputPort,redisMaxOutput.inputInd);

        SmtpOutputOperator smtpOutputOperator = getSmtpOutputOperator("SmtpCalcOperator", dag, conf);
        dag.addStream("calc_alert_mail", oper.smtpAlert,smtpOutputOperator.input);

        return oper;
    }

    /**
     * Create the DAG
     */
    @SuppressWarnings("unchecked")
    @Override
    public void populateDAG(DAG dag, Configuration conf) {

        if (LAUNCHMODE_YARN.equals(conf.get(DAG.LAUNCH_MODE))) {
            // settings only affect distributed mode
            dag.getAttributes().attr(DAG.CONTAINER_MEMORY_MB).setIfAbsent(2048);
            dag.getAttributes().attr(DAG.MASTER_MEMORY_MB).setIfAbsent(1024);
            //dag.getAttributes().attr(DAG.CONTAINERS_MAX_COUNT).setIfAbsent(1);
        } else if (LAUNCHMODE_LOCAL.equals(conf.get(DAG.LAUNCH_MODE))) {
        }

        dag.setAttribute(DAG.APPLICATION_NAME, "MachineData-DemoApplication");
        dag.setAttribute(DAG.DEBUG, false);
        dag.getAttributes().attr(DAG.STREAMING_WINDOW_SIZE_MILLIS).set(streamingWindowSizeMilliSeconds);

        RandomInformationTupleGenerator randomGen = getRandomInformationTupleGenerator("RandomInfoGenerator", dag);
        setDefaultOutputPortQueueCapacity(dag,randomGen.outputInline);
        setDefaultOutputPortQueueCapacity(dag,randomGen.output);

        //dag.setAttribute(randomGen, OperatorContext.INITIAL_PARTITION_COUNT, 16);

        if(conf.getBoolean("machinedata.calculate.average",true)){
            MachineInfoAveragingPrerequisitesOperator prereqAverageOper=addAverageCalculation(dag, conf);
            dag.addStream("random_to_avg", randomGen.outputInline, prereqAverageOper.inputPort).setLocality(DAG.Locality.CONTAINER_LOCAL);
        }

        CalculatorOperator calculatorOperator =addCalculator(dag,conf);
        dag.addStream("random_to_calculator", randomGen.output,calculatorOperator.dataPort);

        if(conf.getBoolean("machinedata.calculate.percentile",false)){
             calculatorOperator.setComputePercentile(true);
        }

        if(conf.getBoolean("machinedata.calculate.sd",false)){
            calculatorOperator.setComputeSD(true);
        }

        if(conf.getBoolean("machinedata.calculate.max",false)){
            calculatorOperator.setComputeMax(true);
        }
    }
}
