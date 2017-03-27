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

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.ApplicationTraceIndexDao;
import com.navercorp.pinpoint.collector.util.EsTables;
import com.navercorp.pinpoint.collector.util.JsonUtils;
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
        if (logger.isDebugEnabled()) {
            logger.debug("insert:{}", span);
        }
        Map<String,Object> map = new HashMap<String,Object>();
        map.put("elapsed", span.getElapsed());
        map.put("err", span.getErr());
        map.put("agentId", span.getAgentId());
        
        IndexResponse response = client.prepareIndex(EsTables.APPLICATION_TRACE_INDEX,EsTables.APPLICATION_TRACE_INDEX_CF_TRACE).setSource(JsonUtils.encode(map)).execute().actionGet();
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

}
