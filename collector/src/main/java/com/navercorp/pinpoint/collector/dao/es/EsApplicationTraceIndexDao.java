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

import static com.navercorp.pinpoint.common.hbase.HBaseTables.AGENT_NAME_MAX_LEN;
import static com.navercorp.pinpoint.common.hbase.HBaseTables.APPLICATION_TRACE_INDEX;
import static com.navercorp.pinpoint.common.hbase.HBaseTables.APPLICATION_TRACE_INDEX_CF_TRACE;

import org.apache.hadoop.hbase.client.Put;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.ApplicationTraceIndexDao;
import com.navercorp.pinpoint.common.buffer.AutomaticBuffer;
import com.navercorp.pinpoint.common.buffer.Buffer;
import com.navercorp.pinpoint.thrift.dto.TSpan;

/**
 * 
 * @author yangjian
 */
@Repository
public class EsApplicationTraceIndexDao implements ApplicationTraceIndexDao {
	
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
    
	@Autowired  
    private Client client;
    
    @Override
    public void insert(final TSpan span) {
        if (span == null) {
            throw new NullPointerException("span must not be null");
        }
        logger.debug("EsApplicationTraceIndexDao insert"+span.toString());
        
        final Buffer buffer = new AutomaticBuffer(10 + AGENT_NAME_MAX_LEN);
        buffer.putVar(span.getElapsed());
        buffer.putSVar(span.getErr());
        buffer.putPrefixedString(span.getAgentId());
        final byte[] value = buffer.getBuffer();

        long acceptedTime = acceptedTimeService.getAcceptedTime();
        final byte[] distributedKey = crateRowKey(span, acceptedTime);
        Put put = new Put(distributedKey);

        put.addColumn(APPLICATION_TRACE_INDEX_CF_TRACE, makeQualifier(span) , acceptedTime, value);

        hbaseTemplate.put(APPLICATION_TRACE_INDEX, put);
        
        
    }

}
