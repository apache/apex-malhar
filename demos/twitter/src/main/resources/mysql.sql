DROP TABLE if exists tweets;
CREATE TABLE tweets (
   window_id LONG NOT NULL,
   creation_date DATE,
   text VARCHAR(256) NOT NULL,
   userid VARCHAR(40) NOT NULL,
   KEY ( userid, creation_date)
   );

drop table if exists dt_window_id_tracker;
CREATE TABLE dt_window_id_tracker (
  dt_application_id VARCHAR(100) NOT NULL,
  dt_operator_id int(11) NOT NULL,
  dt_window_id bigint NOT NULL,
  UNIQUE (dt_application_id, dt_operator_id, dt_window_id)
)  ENGINE=MyISAM DEFAULT CHARSET=latin1;
