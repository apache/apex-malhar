package org.apache.apex.malhar.lib.state.spillable;

import org.apache.apex.malhar.lib.state.TimeSlicedBucketedState;
import org.apache.apex.malhar.lib.state.managed.TimeBucketAssigner;

/**
 * Created by david on 8/31/16.
 */
public interface SpillableTimeStateStore extends TimeSlicedBucketedState, SpillableStateStore
{
  void setTimeBucketAssigner(TimeBucketAssigner timeBucketAssigner);
}
