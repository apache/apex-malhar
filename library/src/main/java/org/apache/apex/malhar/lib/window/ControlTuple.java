package org.apache.apex.malhar.lib.window;

/**
 * Control tuple interface.
 * TODO: This should be removed or moved to Apex Core when Apex Core has native support for custom control tuples.
 */
public interface ControlTuple
{
  /**
   * Watermark control tuple
   */
  interface Watermark extends ControlTuple
  {
    /**
     * Gets the timestamp associated with this watermark
     *
     * @return
     */
    long getTimestamp();
  }
}
