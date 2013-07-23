<?php
/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */

header("Content-type: application/json");
$redis = new Redis();
$redis->connect('127.0.0.1');
$from = $_GET['from'];
$bucket = $_GET['bucket'];
$publisher = $_GET['publisher'];
$advertiser = $_GET['advertiser'];
$adunit = $_GET['adunit'];


// result array  
$result = array();

// get data by days first
$format = 'Ymd';
while ($from < time()) {
  $date = gmdate($format, $from);
  $key = 'd|' . $date;
  if ($publisher != '') {
      $key = $key . "|0:" . $publisher;
  } else {
      $key = $key . "|0:*" ;
  }
  if ($advertiser != '') {
      $key = $key . "|1:" . $advertiser;
  } else {
      $key = $key . "|1:*" ;
  }
  if ($adunit != '') {
      $key = $key . "|2:" . $adunit;
  } else {
      $key = $key . "|2:*" ;
  }
  $keys = $redis->keys($key);
  $cost = 0;
  $revenue = 0;
  $impressions = 0;  
  $clicks = 0;   
  foreach ($keys as $key)
  {
	  $hash = $redis->hGetAll($key);
	  if ($hash) {
	    $cost += $hash['1'];
	    $revenue += $hash['2'];
	    $impressions += $hash['3'];
	    $clicks += $hash['4'];
	  }
  }
  if (count($keys) > 0) {
  	$result[] = array('timestamp'=> $from * 1000, 'cost'=>$cost, 'revenue'=>$revenue, 'clicks'=>$clicks, 'impressions'=>$impressions);
  }
  $from += 3600 * 24;
}

// get data by hours 
$from = $from - 3600 * 24;
$format = 'YmdH';
while ($from < time()) {
  $date = gmdate($format, $from);
  $key = 'h|' . $date;
  if ($publisher != '') {
      $key = $key . "|0:" . $publisher;
  } else {
      $key = $key . "|0:*" ;
  }
  if ($advertiser != '') {
      $key = $key . "|1:" . $advertiser;
  } else {
      $key = $key . "|1:*" ;
  }
  if ($adunit != '') {
      $key = $key . "|2:" . $adunit;
  } else {
      $key = $key . "|2:*" ;
  }
  $keys = $redis->keys($key);
  $cost = 0;
  $revenue = 0;
  $impressions = 0;  
  $clicks = 0;   
  foreach ($keys as $key)
  {
	  $hash = $redis->hGetAll($key);
	  if ($hash) {
	    $cost += $hash['1'];
	    $revenue += $hash['2'];
	    $impressions += $hash['3'];
	    $clicks += $hash['4'];
	  }
  }
  if (count($keys) > 0) {
  	$result[] = array('timestamp'=> $from * 1000, 'cost'=>$cost, 'revenue'=>$revenue, 'clicks'=>$clicks, 'impressions'=>$impressions);
  }
  $from += 3600;
}

// get data by minutes
$from = $from - 3600;
$format = 'YmdHi';
while ($from < (time()-120)) {
  $date = gmdate($format, $from);
  $key = 'm|' . $date;
  if ($publisher != '') {
      $key = $key . "|0:" . $publisher;
  } else {
      $key = $key . "|0:*" ;
  }
  if ($advertiser != '') {
      $key = $key . "|1:" . $advertiser;
  } else {
      $key = $key . "|1:*" ;
  }
  if ($adunit != '') {
      $key = $key . "|2:" . $adunit;
  } else {
      $key = $key . "|2:*" ;
  }
  $keys = $redis->keys($key);
  $cost = 0;
  $revenue = 0;
  $impressions = 0;  
  $clicks = 0;   
  foreach ($keys as $key)
  {
	  $hash = $redis->hGetAll($key);
	  if ($hash) {
	    $cost += $hash['1'];
	    $revenue += $hash['2'];
	    $impressions += $hash['3'];
	    $clicks += $hash['4'];
	  }
  }
  if (count($keys) > 0) {
  	$result[] = array('timestamp'=> $from * 1000, 'cost'=>$cost, 'revenue'=>$revenue, 'clicks'=>$clicks, 'impressions'=>$impressions);
  }
  $from += 60;
}
print json_encode($result);

?>
