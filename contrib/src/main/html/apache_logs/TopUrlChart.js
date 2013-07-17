/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */

/**
 * Functions fro charting top url table.
 * @author Dinesh Prasad (dinesh@malhar-inc.com) 
 */

function DrawTopUrlTableChart()
{
  try
  {
    var connect = new XMLHttpRequest();
    connect.onreadystatechange = function() {
      if(connect.readyState==4 && connect.status==200) {
        var data = connect.response;
        var pts = JSON.parse(data);
        topUrlTable = new google.visualization.DataTable();
        topUrlTable.addColumn('string', 'URL');
        topUrlTable.addColumn('number', 'requests per sec');
        topUrlTable.addRows(10);
        for(var i=0; (i <  pts.length)&&(i < 10); i++) 
        {
          var row = pts[i].split("##");
          topUrlTable.setCell(i, 0, row[0]);
          topUrlTable.setCell(i, 1, parseInt(row[1]));
          delete pts[i];
        }
        topUrlTableChart.draw(topUrlTable, {showRowNumber: true});
        delete topUrlTable;
        delete data;
        delete pts;
      }
    }
    connect.open('GET',  "TopUrlData.php", true);
    connect.send(null);
  } catch(e) {
  }
}
