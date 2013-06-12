<?php
header("Content-type: application/json");
$redis = new Redis();
$redis->connect('127.0.0.1');
$redis->select(6);

// result array
$result = array();


$value = $redis->get("total");
//var_dump($value);
print $value;

?>
