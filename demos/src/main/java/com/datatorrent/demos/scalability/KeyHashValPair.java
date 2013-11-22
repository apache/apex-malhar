package com.datatorrent.demos.scalability;

import java.util.Map;

import com.datatorrent.lib.util.KeyValPair;

/**
 * <p>KeyHashValPair class.</p>
 *
 * @since 0.9.0
 */
public class KeyHashValPair<V> extends KeyValPair<AggrKey, V>
{

  /**
*
*/
  private static final long serialVersionUID = 7005592007894368002L;

  public KeyHashValPair(AggrKey k, V v)
  {
    super(k, v);
  }

  private KeyHashValPair()
  {
    super(null, null);
  }

  @Override
  public int hashCode()
  {
    int hashcode = 0;
    AggrKey k = getKey();
    if(k != null){
      if(k.getAdUnit() != null)
        hashcode += k.getAdUnit();
      if(k.getAdvertiserId() != null)
        hashcode += k.getAdvertiserId();
      if(k.getPublisherId() != null)
        hashcode += k.getPublisherId();
      hashcode = (hashcode * (k.getParitions()))/(InputItemGenerator.numAdUnits + InputItemGenerator.numAdvertisers + InputItemGenerator.numPublishers) +1;
    }
        
    return hashcode;
  }

  @Override
  public boolean equals(Object o)
  {
    if (!(o instanceof Map.Entry))
      return false;
    Map.Entry e = (Map.Entry) o;
    return (this.getKey() == null ? e.getKey() == null : this.getKey().equals(e.getKey()));
  }

}
