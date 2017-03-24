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

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.AgentInfoDao;
import com.navercorp.pinpoint.collector.mapper.thrift.ThriftBoMapper;
import com.navercorp.pinpoint.collector.util.EsTables;
import com.navercorp.pinpoint.collector.util.JsonUtils;
import com.navercorp.pinpoint.common.bo.AgentInfoBo;
import com.navercorp.pinpoint.common.bo.JvmInfoBo;
import com.navercorp.pinpoint.common.bo.ServerMetaDataBo;
import com.navercorp.pinpoint.thrift.dto.TAgentInfo;
import com.navercorp.pinpoint.thrift.dto.TJvmInfo;
import com.navercorp.pinpoint.thrift.dto.TServerMetaData;

/**
 * @author yangjian
 */
@Repository
public class EsAgentInfoDao implements AgentInfoDao {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired  
    private Client client;

    @Autowired
    @Qualifier("agentInfoBoMapper")
    private ThriftBoMapper<AgentInfoBo, TAgentInfo> agentInfoBoMapper;

    @Autowired
    @Qualifier("serverMetaDataBoMapper")
    private ThriftBoMapper<ServerMetaDataBo, TServerMetaData> serverMetaDataBoMapper;

    @Autowired
    @Qualifier("jvmInfoBoMapper")
    private ThriftBoMapper<JvmInfoBo, TJvmInfo> jvmInfoBoMapper;
    
    @Override
    public void insert(TAgentInfo agentInfo) {

        if (agentInfo == null) {
            throw new NullPointerException("agentInfo must not be null");
        }

        if (logger.isDebugEnabled()) {
            logger.debug("insert agent info. {}", agentInfo);
        }
        AgentInfoBo agentInfoBo = this.agentInfoBoMapper.map(agentInfo);
        IndexResponse response = client.prepareIndex(EsTables.AGENTINFO,EsTables.AGENTINFO_CF_INFO_IDENTIFIER).setSource(JsonUtils.encode(agentInfoBo)).execute().actionGet();
        debugInsert(response);
        if (agentInfo.isSetServerMetaData()) {
            ServerMetaDataBo serverMetaDataBo = this.serverMetaDataBoMapper.map(agentInfo.getServerMetaData());
            response = client.prepareIndex(EsTables.AGENTINFO,EsTables.AGENTINFO_CF_INFO_SERVER_META_DATA).setSource(JsonUtils.encode(serverMetaDataBo)).execute().actionGet();
            debugInsert(response);
        }

        if (agentInfo.isSetJvmInfo()) {
            JvmInfoBo jvmInfoBo = this.jvmInfoBoMapper.map(agentInfo.getJvmInfo());
            response = client.prepareIndex(EsTables.AGENTINFO,EsTables.AGENTINFO_CF_INFO_JVM).setSource(JsonUtils.encode(jvmInfoBo)).execute().actionGet();
            debugInsert(response);
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
