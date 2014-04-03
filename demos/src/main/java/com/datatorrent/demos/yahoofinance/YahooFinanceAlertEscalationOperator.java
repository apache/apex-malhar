/*
* Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.demos.yahoofinance;

import com.datatorrent.lib.util.AlertEscalationOperator;

import java.util.HashMap;
import java.util.Map;

/*
* This operator accepts multiple ticker symbols for which potential alerts are to be raised.
* Once it gets a symbol, it checks the duration since the last alert was raised for this particular ticker.
* If the duration is more than what the alert interval is, then it will raise an alert. This is done for
* each specific ticker symbol.
*
* The functionality is similar to the AlertEscalationOperator, except that this operator can take multiple
* tickers and raise alerts for each one of them based on the time the last alert was raised for a particular symbol
* The AlertEscalationOperator does not consider the time elapsed since the last alert "for a particular ticker"
 *
 */

public class YahooFinanceAlertEscalationOperator extends AlertEscalationOperator {

    protected String lastTicker = "";
    private Map<String, Long> lastAlertMap = new HashMap<String, Long>();

    @Override
    public void processTuple(Object tuple)
    {
       Long lastAlertTimeStamp = 0L;
        long now = System.currentTimeMillis();
        if (inAlertSince < 0) {
            inAlertSince = now;
        }
        lastTupleTimeStamp = now;
        Map<String, String> map = (Map<String, String>) tuple;

        lastTicker = map.get("SYMBOL");

        lastAlertTimeStamp = lastAlertMap.get(lastTicker);
        if (lastAlertTimeStamp == null) {
            lastAlertTimeStamp = 0L;
        }

        if (activated && (lastAlertTimeStamp == 0 || lastAlertTimeStamp + alertInterval < now)) {
            alert.emit(tuple);
            lastAlertTimeStamp = now;
            lastAlertMap.put(lastTicker, lastAlertTimeStamp);

        }
    }
}
