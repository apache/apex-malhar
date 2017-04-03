CREATE DATABASE IF NOT EXISTS testJdbc;

USE testJdbc;

CREATE TABLE IF NOT EXISTS `test_jdbc_table` (
  `ACCOUNT_NO` int(11) NOT NULL,
  `NAME` varchar(255),
  `AMOUNT` int(11));
