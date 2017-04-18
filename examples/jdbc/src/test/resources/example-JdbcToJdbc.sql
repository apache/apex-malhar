DROP DATABASE IF EXISTS testDev;

CREATE DATABASE testDev;

USE testDev;

CREATE TABLE IF NOT EXISTS `test_event_table` (
  `ACCOUNT_NO` int(11) NOT NULL,
  `NAME` varchar(255) DEFAULT NULL,
  `AMOUNT` int(11) DEFAULT NULL
) ENGINE=MyISAM  DEFAULT CHARSET=latin1;

INSERT INTO `test_event_table` (`ACCOUNT_NO`, `NAME`, `AMOUNT`) VALUES
(1, 'User1', 1000),
(2, 'User2', 2000),
(3, 'User3', 3000),
(4, 'User4', 4000),
(5, 'User5', 5000),
(6, 'User6', 6000),
(7, 'User7', 7000),
(8, 'User8', 8000),
(9, 'User9', 9000),
(10, 'User10', 1000);

CREATE TABLE IF NOT EXISTS `test_output_event_table` (
  `ACCOUNT_NO` int(11) NOT NULL,
  `NAME` varchar(255) DEFAULT NULL,
  `AMOUNT` int(11) DEFAULT NULL
) ENGINE=MyISAM  DEFAULT CHARSET=latin1;

CREATE TABLE IF NOT EXISTS `dt_meta` ( 
  `dt_app_id` VARCHAR(100) NOT NULL, 
  `dt_operator_id` INT NOT NULL, 
  `dt_window` BIGINT NOT NULL, 
UNIQUE (`dt_app_id`, `dt_operator_id`, `dt_window`)
) ENGINE=MyISAM  DEFAULT CHARSET=latin1;
