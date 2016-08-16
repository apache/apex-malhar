package org.apache.apex.malhar.lib.utils.serde;

import org.apache.apex.malhar.lib.state.spillable.WindowListener;

public interface ResetableWindowListener extends WindowListener
{
  /**
   * reset for all windows which window id less or equal input windowId
   * this interface doesn't enforce to call reset window for each windows. Several windows can be reset at the same time.
   * @param windowId
   */
  void resetUpToWindow(long windowId);
}
