--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
--

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
