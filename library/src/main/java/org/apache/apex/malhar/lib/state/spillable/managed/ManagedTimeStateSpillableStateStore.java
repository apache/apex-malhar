package org.apache.apex.malhar.lib.state.spillable.managed;

import org.apache.apex.malhar.lib.state.managed.ManagedTimeStateImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableTimeStateStore;

/**
 * Created by david on 8/31/16.
 */
public class ManagedTimeStateSpillableStateStore extends ManagedTimeStateImpl implements SpillableTimeStateStore
{
  public ManagedTimeStateSpillableStateStore()
  {
    super();
  }

}
