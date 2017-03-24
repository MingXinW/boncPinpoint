/*
 * Copyright 2014 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.navercorp.pinpoint.collector.dao.es;

import static com.navercorp.pinpoint.common.hbase.HBaseTables.MAP_STATISTICS_CALLER_VER2_CF_COUNTER;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.MapStatisticsCalleeDao;
import com.navercorp.pinpoint.collector.dao.hbase.statistics.CallerColumnName;
import com.navercorp.pinpoint.collector.dao.hbase.statistics.ColumnName;
import com.navercorp.pinpoint.collector.dao.hbase.statistics.RowInfo;
import com.navercorp.pinpoint.collector.dao.hbase.statistics.RowKeyMerge;
import com.navercorp.pinpoint.collector.util.AcceptedTimeService;
import com.navercorp.pinpoint.collector.util.ConcurrentCounterMap;
import com.navercorp.pinpoint.collector.util.EsTables;
import com.navercorp.pinpoint.collector.util.JsonUtils;
import com.navercorp.pinpoint.common.trace.ServiceType;
import com.navercorp.pinpoint.common.util.ApplicationMapStatisticsUtils;
import com.navercorp.pinpoint.common.util.TimeSlot;
import com.sematext.hbase.wd.RowKeyDistributorByHashPrefix;

/**
 * Update statistics of callee node
 * 
 * @author yangjian
 */
@Repository
public class EsMapStatisticsCalleeDao implements MapStatisticsCalleeDao {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired  
    private Client client;

    @Autowired
    private AcceptedTimeService acceptedTimeService;

    @Autowired
    private TimeSlot timeSlot;

    @Autowired
    @Qualifier("calleeMerge")
    private RowKeyMerge rowKeyMerge;

    @Autowired
    @Qualifier("statisticsCalleeRowKeyDistributor")
    private RowKeyDistributorByHashPrefix rowKeyDistributorByHashPrefix;

    private final boolean useBulk;

    private final ConcurrentCounterMap<RowInfo> counter = new ConcurrentCounterMap<>();

    public EsMapStatisticsCalleeDao() {
        this(true);
    }

    public EsMapStatisticsCalleeDao(boolean useBulk) {
        this.useBulk = useBulk;
    }


    @Override
    public void update(String calleeApplicationName, ServiceType calleeServiceType, String callerApplicationName, ServiceType callerServiceType, String callerHost, int elapsed, boolean isError) {
        if (callerApplicationName == null) {
            throw new NullPointerException("callerApplicationName must not be null");
        }
        if (calleeApplicationName == null) {
            throw new NullPointerException("calleeApplicationName must not be null");
        }

        if (logger.isDebugEnabled()) {
            logger.debug("[Callee] {} ({}) <- {} ({})[{}]",
                    calleeApplicationName, calleeServiceType, callerApplicationName, callerServiceType, callerHost);
        }
        short callerSlotNumber = ApplicationMapStatisticsUtils.getSlotNumber(calleeServiceType, elapsed, isError);
        CallerColumnName callerColumnName = new CallerColumnName(callerServiceType.getCode(), callerApplicationName, callerHost, callerSlotNumber);
        IndexResponse response = client.prepareIndex(EsTables.MAP_STATISTICS_CALLER_VER2,EsTables.MAP_STATISTICS_CALLER_VER2_CF_COUNTER).setSource(JsonUtils.encode(callerColumnName)).execute().actionGet();
        debugInsert(response);
    }

    private void debugInsert(IndexResponse response){
    	String index = response.getIndex();  
        String type = response.getType();  
        String id = response.getId();  
        long version = response.getVersion();  
        boolean created = response.isCreated();  
        logger.debug("EsTraceDao insert:"+index+","+type+","+id+","+version+","+created);  
    }
    
    @Override
    public void flushAll() {
        if (!useBulk) {
            throw new IllegalStateException();
        }

       logger.debug("flushALL");

    }

}
