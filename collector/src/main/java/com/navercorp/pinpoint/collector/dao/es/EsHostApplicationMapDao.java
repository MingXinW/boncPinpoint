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

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.HostApplicationMapDao;
import com.navercorp.pinpoint.collector.util.AcceptedTimeService;
import com.navercorp.pinpoint.collector.util.AtomicLongUpdateMap;
import com.navercorp.pinpoint.collector.util.EsTables;
import com.navercorp.pinpoint.collector.util.JsonUtils;
import com.navercorp.pinpoint.common.buffer.AutomaticBuffer;
import com.navercorp.pinpoint.common.buffer.Buffer;
import com.navercorp.pinpoint.common.hbase.HBaseTables;
import com.navercorp.pinpoint.common.util.TimeSlot;
import com.navercorp.pinpoint.common.util.TimeUtils;
import com.sematext.hbase.wd.AbstractRowKeyDistributor;

/**
 * 
 * @author yangjian
 */
@Repository
public class EsHostApplicationMapDao implements HostApplicationMapDao {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired  
    private Client client;
    
    @Autowired
    private AcceptedTimeService acceptedTimeService;

    @Autowired
    private TimeSlot timeSlot;

    @Autowired
    @Qualifier("acceptApplicationRowKeyDistributor")
    private AbstractRowKeyDistributor rowKeyDistributor;

    // FIXME should modify to save a cachekey at each 30~50 seconds instead of saving at each time
    private final AtomicLongUpdateMap<CacheKey> updater = new AtomicLongUpdateMap<>();

    @Override
    public void insert(String host, String bindApplicationName, short bindServiceType, String parentApplicationName, short parentServiceType) {
        if (host == null) {
            throw new NullPointerException("host must not be null");
        }
        if (bindApplicationName == null) {
            throw new NullPointerException("bindApplicationName must not be null");
        }
        final long statisticsRowSlot = getSlotTime();

        final CacheKey cacheKey = new CacheKey(host, bindApplicationName, bindServiceType, parentApplicationName, parentServiceType);
        final boolean needUpdate = updater.update(cacheKey, statisticsRowSlot);
        if (needUpdate) {
        	 if (logger.isDebugEnabled()) {
                 logger.debug("Insert host-application map. host={}, bindApplicationName={}, bindServiceType={}, parentApplicationName={}, parentServiceType={}",
                         host, bindApplicationName, bindServiceType, parentApplicationName, parentServiceType);
             }
        	 Map<String,Object> map = new HashMap<String,Object>();
        	 map.put("host", host);
        	 map.put("bindApplicationName", bindApplicationName);
        	 map.put("bindServiceType", bindServiceType);
        	 map.put("parentApplicationName", parentApplicationName);
        	 map.put("parentServiceType", parentServiceType);
        	 try {
                 IndexResponse response = client.prepareIndex(EsTables.HOST_APPLICATION_MAP_VER2,EsTables.HOST_APPLICATION_MAP_VER2_CF_MAP).setSource(JsonUtils.encode(map)).execute().actionGet();
                 debugInsert(response);
             } catch (Exception ex) {
                 logger.warn("retry one. Caused:{}", ex.getCause(), ex);
                 IndexResponse response = client.prepareIndex(EsTables.HOST_APPLICATION_MAP_VER2,EsTables.HOST_APPLICATION_MAP_VER2_CF_MAP).setSource(JsonUtils.encode(map)).execute().actionGet();
                 debugInsert(response);
             }
        }
    }
    
    private long getSlotTime() {
        final long acceptedTime = acceptedTimeService.getAcceptedTime();
        return timeSlot.getTimeSlot(acceptedTime);
    }


    private byte[] createColumnName(String host, String bindApplicationName, short bindServiceType) {
        Buffer buffer = new AutomaticBuffer();
        buffer.putPrefixedString(host);
        buffer.putPrefixedString(bindApplicationName);
        buffer.put(bindServiceType);
        return buffer.getBuffer();
    }


    private byte[] createRowKey(String parentApplicationName, short parentServiceType, long statisticsRowSlot, String parentAgentId) {
        final byte[] rowKey = createRowKey0(parentApplicationName, parentServiceType, statisticsRowSlot, parentAgentId);
        return  rowKeyDistributor.getDistributedKey(rowKey);
    }

    byte[] createRowKey0(String parentApplicationName, short parentServiceType, long statisticsRowSlot, String parentAgentId) {

        // even if  a agentId be added for additional specifications, it may be safe to scan rows.
        // But is it needed to add parentAgentServiceType?
        final int SIZE = HBaseTables.APPLICATION_NAME_MAX_LEN + 2 + 8;
        final Buffer rowKeyBuffer = new AutomaticBuffer(SIZE);
        rowKeyBuffer.putPadString(parentApplicationName, HBaseTables.APPLICATION_NAME_MAX_LEN);
        rowKeyBuffer.put(parentServiceType);
        rowKeyBuffer.put(TimeUtils.reverseTimeMillis(statisticsRowSlot));
        // there is no parentAgentId for now.  if it added later, need to comment out below code for compatibility.
//        rowKeyBuffer.putPadString(parentAgentId, HBaseTables.AGENT_NAME_MAX_LEN);
        return rowKeyBuffer.getBuffer();
    }

    private static final class CacheKey {
        private final String host;
        private final String applicationName;
        private final short serviceType;

        private final String parentApplicationName;
        private final short parentServiceType;

        public CacheKey(String host, String applicationName, short serviceType, String parentApplicationName, short parentServiceType) {
            if (host == null) {
                throw new NullPointerException("host must not be null");
            }
            if (applicationName == null) {
                throw new NullPointerException("bindApplicationName must not be null");
            }
            this.host = host;
            this.applicationName = applicationName;
            this.serviceType = serviceType;

            // may be null for below two parent values.
            this.parentApplicationName = parentApplicationName;
            this.parentServiceType = parentServiceType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CacheKey cacheKey = (CacheKey) o;

            if (parentServiceType != cacheKey.parentServiceType) return false;
            if (serviceType != cacheKey.serviceType) return false;
            if (!applicationName.equals(cacheKey.applicationName)) return false;
            if (!host.equals(cacheKey.host)) return false;
            if (parentApplicationName != null ? !parentApplicationName.equals(cacheKey.parentApplicationName) : cacheKey.parentApplicationName != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = host.hashCode();
            result = 31 * result + applicationName.hashCode();
            result = 31 * result + (int) serviceType;
            result = 31 * result + (parentApplicationName != null ? parentApplicationName.hashCode() : 0);
            result = 31 * result + (int) parentServiceType;
            return result;
        }
    }



    private void debugInsert(IndexResponse response){
    	String index = response.getIndex();  
        String type = response.getType();  
        String id = response.getId();  
        long version = response.getVersion();  
        boolean created = response.isCreated();  
        logger.debug("EsTraceDao insert:"+index+","+type+","+id+","+version+","+created);  
    }

}
