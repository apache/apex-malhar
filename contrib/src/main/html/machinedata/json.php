<?php
/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */

header("Content-type: application/json");
$redis = new Redis();
$redis->connect('localhost');
$redis->select(15);
$from = $_GET['from'];
$bucket = $_GET['bucket'];
$customer = $_GET['customer'];
$product = $_GET['product'];
$os = $_GET['os'];
$software1 = $_GET['software1'];
$software2 = $_GET['software2'];
$software3 = $_GET['software3'];

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
if ($customer != '') {
  $arr[] = "0:".$customer;
} 
if ($product != '') {
  $arr[] = "1:".$product;
} 
if ($os != '') {
  $arr[] = "2:".$os;
} 
if ($software1 != '') {
  $arr[] = "3:".$software1;
} 
if ($software2 != '') {
  $arr[] = "4:".$software2;
} 
if ($software3 != '') {
  $arr[] = "5:".$software3;
} 
$subpattern = "";
if (count($arr) != 0) {
  $subpattern = join("|", $arr);
}

$result = array();

while ($from < time()) {
  $date = gmdate($format, $from);
  if ($subpattern != '') {
    $key = $bucket . '|' . $date . '|' . $subpattern;
  } else {
    $key = $bucket . '|' . $date ;
  }
  $hash = $redis->hGetAll($key);
  if ($hash) {
    $cpu = $hash['cpu'];
    $ram = $hash['ram'];
    $hdd = $hash['hdd'];
    $result[] = array('timestamp'=> $from * 1000, 'cpu'=>$cpu, 'ram'=>$ram, 'hdd'=>$hdd);
  }
  $from += $incr;
}

array_pop($result);
print json_encode($result);

?>
