package com.datatorrent.lib.sql;


public class LeftOuterJoinOperator extends InnerJoinOperator
{
	@Override
  public void endWindow()
  {
		// Check join of each row
		for (int i=0; i < table1.size(); i++) {
			boolean merged = false;
			for (int j=0; j < table2.size(); j++) {
				if ((joinCondition == null) || (joinCondition.isValidJoin(table1.get(i), table2.get(j)))) {
					joinRows(table1.get(i), table2.get(j));
					merged = true;
				} 
			}
			if (!merged) {
				joinRows(table1.get(i), null);
			}
		}
  }
}
