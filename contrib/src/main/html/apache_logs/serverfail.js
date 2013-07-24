/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */

/**
 * Functions fro charting top url table.
 * @author Dinesh Prasad (dinesh@malhar-inc.com) 
 */

function DrawServer404TableChart()
{
  try
  {
    var connect = new XMLHttpRequest();
    connect.onreadystatechange = function() {
      if(connect.readyState==4 && connect.status==200) {
        var data = connect.response;
        var pts = JSON.parse(data);
        var server404 = new google.visualization.DataTable();
        server404.addColumn('string', 'SERVER');
        server404.addColumn('number', '404 per sec');
        server404.addRows(10);
        for(var i=0; ((i <  pts.length)&&(i < 10)); i++) 
        {
          var row = pts[i].split("##");
          if ((row[0] == null)||(row[0] == ""))
          {
            server404.setCell(i, 0, "-");
          } else {
            server404.setCell(i, 0, row[0]);
          }
          if ((row[1] == null)||(row[1] == ""))
          {
            server404.setCell(i, 1, 0);
          } else {
            server404.setCell(i, 1, parseInt(row[1]));
          }
        }
        //document.getElementById('risky_client_div').innerHTML = data;
        //document.getElementById('risky_client_div').innerHTML = server404.getNumberOfRows();
        server404TableChart.draw(server404, {showRowNumber: true});
        delete server404;
        delete data;
        delete pts;
      }
    }
    connect.open('GET',  "Server404.php", true);
    connect.send(null);
  } catch(e) {
  }
}
