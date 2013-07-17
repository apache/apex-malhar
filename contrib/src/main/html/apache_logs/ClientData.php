<?php
header("Content-type: application/json");
$redis = new Redis();
$redis->connect('node5.morado.com');
$redis->select(9);

// result array
$result = array();

$value = $redis->get(1);
$result[] = $value;

print json_encode($result);

?>
