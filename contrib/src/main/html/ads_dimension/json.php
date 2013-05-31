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

switch ($bucket) {
case 'D':
  $format = 'Ymd';
  $incr = 60 * 60 * 24;
  break;
case 'h':
  $format = 'YmdH';
  $incr = 60 * 60;
  break;
case 'm':
  $format = 'YmdHi';
  $incr = 60;
  break;
default:
  break;
}

$arr = array();
if ($publisher != '') {
  $arr[] = "0:".$publisher;
}
if ($advertiser != '') {
  $arr[] = "1:".$advertiser;
}
if ($adunit != '') {
  $arr[] = "2:".$adunit;
}
$subpattern = "";
if (count($arr) != 0) {
  $subpattern = join("|", $arr);
}

$result = array();

while ($from < time()) {
  $date = gmdate($format, $from);
  $key = $bucket . '|' . $date . '|' . $subpattern;
  $hash = $redis->hGetAll($key);
  if ($hash) {
    $cost = $hash['1'];
    $revenue = $hash['2'];
    $impressions = $hash['3'];
    $clicks = $hash['4'];
    $result[] = array('timestamp'=> $from * 1000, 'cost'=>$cost, 'revenue'=>$revenue, 'clicks'=>$clicks, 'impressions'=>$impressions);
  }
  $from += $incr;
}

print json_encode($result);

?>
