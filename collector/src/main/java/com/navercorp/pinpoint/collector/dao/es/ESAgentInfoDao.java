package com.navercorp.pinpoint.collector.dao.es;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.navercorp.pinpoint.collector.dao.AgentInfoDao;
import com.navercorp.pinpoint.collector.dao.es.base.EsClient;
import com.navercorp.pinpoint.collector.mapper.thrift.ThriftBoMapper;
import com.navercorp.pinpoint.collector.util.EsIndexs;
import com.navercorp.pinpoint.common.server.bo.AgentInfoBo;
import com.navercorp.pinpoint.common.server.bo.JvmInfoBo;
import com.navercorp.pinpoint.common.server.bo.ServerMetaDataBo;
import com.navercorp.pinpoint.common.util.TimeUtils;
import com.navercorp.pinpoint.thrift.dto.TAgentInfo;
import com.navercorp.pinpoint.thrift.dto.TJvmInfo;
import com.navercorp.pinpoint.thrift.dto.TServerMetaData;


@Repository("esAgentInfoDao")
public class ESAgentInfoDao implements AgentInfoDao {
	
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

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

        //byte[] agentId = Bytes.toBytes(agentInfo.getAgentId());
        long reverseKey = TimeUtils.reverseTimeMillis(agentInfo.getStartTimestamp());
        //byte[] rowKey = RowKeyUtils.concatFixedByteAndLong(agentId, HBaseTables.AGENT_NAME_MAX_LEN, reverseKey);

        // should add additional agent informations. for now added only starttime for sqlMetaData
        AgentInfoBo agentInfoBo = this.agentInfoBoMapper.map(agentInfo);
        AgentInfoBo.Builder builder = agentInfoBo.toBuilder();
        if (agentInfo.isSetServerMetaData()) {
            ServerMetaDataBo serverMetaDataBo = this.serverMetaDataBoMapper.map(agentInfo.getServerMetaData());
            builder.setServerMetaData(serverMetaDataBo);
        }

        if (agentInfo.isSetJvmInfo()) {
            JvmInfoBo jvmInfoBo = this.jvmInfoBoMapper.map(agentInfo.getJvmInfo());
            builder.setJvmInfo(jvmInfoBo);
        }
       
		try {
			 agentInfoBo = builder.build();
			 ObjectMapper mapper = new ObjectMapper();
		     byte[] json = mapper.writeValueAsBytes(agentInfoBo);
		     String id = agentInfo.getAgentId() + EsIndexs.ID_SEP + reverseKey;
				// TODO Auto-generated method stub
			EsClient.client().prepareIndex(EsIndexs.AGENT_INFO,EsIndexs.TYPE, id)
					.setSource(json).get();
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			logger.error("esAgentInfoDao insert error. Cause:{}", e.getMessage(), e);
		}
	}

}
