package com.datatorrent.lib.pigquery;

import java.util.Map;


public class ThreeWayPigSplit  extends PigSplitOperator<Map<String, Integer>>
{

  public ThreeWayPigSplit()
  {
    super(3);
  }

  @Override
  public boolean isValidEmit(int i, Map<String, Integer> tuple)
  {
    if (i == 0) {
      if (tuple.get("f1") < 7) return true;
    }
    if (i == 1) {
      if (tuple.get("f2") == 5) return true;
    }
    if (i == 2) {
      if (tuple.get("f3") < 6) return true;
    }
    return false;
  }

}
