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
import com.datatorrent.contrib.redis.RedisOutputOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.SmtpOutputOperator;
import com.datatorrent.lib.math.AverageKeyVal;
import com.datatorrent.lib.util.KeyValPair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

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
        MachineInfoBucketOperator oper = dag.addOperator(name, MachineInfoBucketOperator.class);
        return oper;
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

    public RedisOutputOperator<MachineKey, Double> getRedisOutputOperator(String name, DAG dag, Configuration conf) {
        RedisOutputOperator<MachineKey, Double> oper = dag.addOperator(name, new RedisOutputOperator<MachineKey, Double>());
        String host = conf.get("machinedata.redis.host", "localhost");
        int port = conf.getInt("machinedata.redis.port", 6379);
        int database = conf.getInt("machinedata.redis.db", 15);
        oper.setHost(host);
        oper.setPort(port);
        oper.selectDatabase(database);
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
    	dag.setOutputPortAttribute(randomGen.outputInline, PortContext.QUEUE_CAPACITY, 32 * 1024);
    	dag.setAttribute(randomGen, OperatorContext.INITIAL_PARTITION_COUNT, 16);

//        MachineInfoAverageOperator machineInfoAverageOperator = getMachineInfoAverageOperator("MachineInfoAveOperator", dag);
        MachineInfoAveragingPrerequisitesOperator machineInfoAveragePrereqOperator = getMachineInfoAveragingPrerequisitesOperator("MachineInfoAvePrereqOperator", dag);
    	dag.setInputPortAttribute(machineInfoAveragePrereqOperator.inputPort, PortContext.PARTITION_PARALLEL, true);
    	dag.setInputPortAttribute(machineInfoAveragePrereqOperator.inputPort, PortContext.QUEUE_CAPACITY, 32 * 1024);
    	dag.setOutputPortAttribute(machineInfoAveragePrereqOperator.outputPort, PortContext.QUEUE_CAPACITY, 32 * 1024);

        MachineInfoAveragingOperator machineInfoAveragingOperator = getMachineInfoAveragingOperator("MachineInfoAveOperator", dag);
       	dag.setInputPortAttribute(machineInfoAveragingOperator.inputPort, PortContext.QUEUE_CAPACITY, 32 * 1024);
        dag.setOutputPortAttribute(machineInfoAveragingOperator.outputPort, PortContext.QUEUE_CAPACITY, 32 * 1024);

        AlertGeneratorOperator alertGeneratorOperator = getAlertGeneratorOperator("AlertGen", dag);

        RedisOutputOperator redisOperator = getRedisOutputOperator("RedisOutput", dag, conf);
        dag.setInputPortAttribute(redisOperator.inputInd, PortContext.QUEUE_CAPACITY, 32 * 1024);

        //RedisOutputOperator redisAlertOperator = getRedisOutputOperator("RedisAlertOutput", dag, conf);
        SmtpOutputOperator smtpOutputOperator = getSmtpOutputOperator("SmtpOperator", dag, conf);

//        dag.addStream("random_minfo_gen", randomGen.machine, machineInfoAverageOperator.machineInputPort).setInline(true);
//        dag.addStream("redis_output", machineInfoAverageOperator.outputPort, redisOperator.inputInd);
        dag.addStream("random_minfo_gen", randomGen.outputInline, machineInfoAveragePrereqOperator.inputPort).setInline(true);
        dag.addStream("minfo_prereq_avg", machineInfoAveragePrereqOperator.outputPort, machineInfoAveragingOperator.inputPort);
        dag.addStream("alert_gen",alertGeneratorOperator.alertPort, machineInfoAveragingOperator.alertPort);
        dag.addStream("redis_output", machineInfoAveragingOperator.outputPort, redisOperator.inputInd);
        //dag.addStream("redis_alert", randomGen.alert, redisAlertOperator.inputInd);
        //dag.addStream("alert_mail", randomGen.smtpAlert, smtpOutputOperator.input);
        dag.addStream("alert_mail", machineInfoAveragingOperator.smtpAlert, smtpOutputOperator.input);
    }
}
