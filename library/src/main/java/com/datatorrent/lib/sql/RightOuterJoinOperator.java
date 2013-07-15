package com.datatorrent.lib.sql;


public class RightOuterJoinOperator extends InnerJoinOperator
{
	@Override
  public void endWindow()
  {
		// Check join of each row
		for (int i=0; i < table2.size(); i++) {
			boolean merged = false;
			for (int j=0; j < table1.size(); j++) {
				if ((joinCondition == null) || (joinCondition.isValidJoin(table1.get(j), table2.get(i)))) {
					joinRows(table1.get(j), table2.get(i));
					merged = true;
				} 
			}
			if (!merged) {
				joinRows(null, table2.get(i));
			}
		}
  }
}
