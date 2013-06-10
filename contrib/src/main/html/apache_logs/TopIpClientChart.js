/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */

/**
 * Functions fro charting top IpClient table.
 * @author Dinesh Prasad (dinesh@malhar-inc.com) 
 */

function DrawTopIpClientTableChart()
{
  try
  {
    var connect = new XMLHttpRequest();
    connect.onreadystatechange = function() {
      if(connect.readyState==4 && connect.status==200) {
        var data = connect.response;
        var pts = JSON.parse(data);
        if (pts.length > topIpClientTable.getNumberOfRows())
        {
          var numRows = pts.length - topIpClientTable.getNumberOfRows();
          topIpClientTable.addRows(numRows);      
        } 
        for(var i=0; i <  pts.length; i++) 
        {
          topIpClientTable.setCell(0, i, pts[i]);
          delete pts[i];
        }
        delete pts;
        delete data;
        topIpClientTableChart.draw(topIpClientTable, {showRowNumber: true});
        //document.getElementById('top_IpClient_div').innerHTML = data;
      }
    }
    connect.open('GET',  "/TopIpClientData.php", true);
    connect.send(null);
  } catch(e) {
  }
}
