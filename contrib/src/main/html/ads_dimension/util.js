/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */

/**
 * Utility functions for charting.
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
 */

/**
 * Function to get parameters from query url.
 */
function QueryString() {
  var query_string = {};
  var query = window.location.search.substring(1);
  return query;
}
function SplitQuery(query)
{  
	var params = new Array();
	var vars = query.split("&");
	for (var i=0;i<vars.length;i++)
	{
		var pair = vars[i].split("=");
		if(pair.length == 2) 
		{
			params[pair[0]] = pair[1];
		}
	}
	return params;
}  
