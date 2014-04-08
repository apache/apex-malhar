/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.frauddetect;

import com.datatorrent.api.LocalMode;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Fraud detection application test
 */
public class FrauddetectApplicationTest {

    public FrauddetectApplicationTest() {
    }

    @Test
    public void testApplication() throws Exception {
        Application application = new Application();

        Configuration conf = new Configuration(false);
        conf.set(Application.MONGO_HOST_PROPERTY, "localhost");
        conf.set(Application.MONGO_HOST_PROPERTY, "localhost");
        conf.setInt(Application.BIN_THRESHOLD_PROPERTY, 20);
        conf.setInt(Application.AVG_THRESHOLD_PROPERTY, 1200);
        conf.setInt(Application.CC_THRESHOLD_PROPERTY, 420);
        LocalMode lma = LocalMode.newInstance();

        application.populateDAG(lma.getDAG(), conf);
        lma.getController().run(120000);
    }

}
