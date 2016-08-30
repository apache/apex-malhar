package org.apache.apex.malhar.lib.state.spillable;

import org.apache.apex.malhar.lib.state.TimeSlicedBucketedState;
import org.apache.apex.malhar.lib.state.managed.TimeBucketAssigner;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;
import com.datatorrent.api.Operator;

/**
 * Created by david on 8/31/16.
 */
public interface SpillableTimeStateStore extends TimeSlicedBucketedState, Component<Context.OperatorContext>,
    Operator.CheckpointNotificationListener, WindowListener
{
  void setTimeBucketAssigner(TimeBucketAssigner timeBucketAssigner);
}
