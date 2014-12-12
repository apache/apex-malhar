package com.datatorrent.demos.udpecho;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;

/**
 * Created by Pramod Immaneni <pramod@datatorrent.com> on 12/11/14.
 */
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    MessageReceiver receiver = dag.addOperator("Message Receiver", MessageReceiver.class);
    MessageResponder responder = dag.addOperator("Message Responder", MessageResponder.class);
    dag.addStream("messages", receiver.messageOutput, responder.messageInput).setLocality(DAG.Locality.CONTAINER_LOCAL);
  }
}
