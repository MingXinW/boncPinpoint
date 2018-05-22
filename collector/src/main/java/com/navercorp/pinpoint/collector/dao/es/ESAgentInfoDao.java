package com.navercorp.pinpoint.collector.dao.es;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.navercorp.pinpoint.collector.dao.AgentInfoDao;
import com.navercorp.pinpoint.collector.dao.es.base.EsClient;
import com.navercorp.pinpoint.collector.mapper.thrift.ThriftBoMapper;
import com.navercorp.pinpoint.collector.util.BeanToJson;
import com.navercorp.pinpoint.collector.util.EsIndexs;
import com.navercorp.pinpoint.common.server.bo.AgentInfoBo;
import com.navercorp.pinpoint.common.server.bo.JvmInfoBo;
import com.navercorp.pinpoint.common.server.bo.ServerMetaDataBo;
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
			
			String id = agentInfo.getAgentId() + EsIndexs.ID_SEP +  agentInfo.getStartTimestamp();
			JSONObject jsonbject = BeanToJson.toEsTime(agentInfoBo);

			GetResponse response = EsClient.client().prepareGet(EsIndexs.AGENT_INFO, EsIndexs.TYPE, id).get();
			if(null == agentInfoBo.getServerMetaData()) {
				jsonbject.put("serverMetaData", response.getSourceAsMap().get("serverMetaData"));
			}
			
			if(null == agentInfoBo.getJvmInfo()) {
				jsonbject.put("jvmInfo", response.getSourceAsMap().get("jvmInfo"));
			}
			
			EsClient.client().prepareIndex(EsIndexs.AGENT_INFO, EsIndexs.TYPE, id)
					.setSource(jsonbject.toJSONString(), XContentType.JSON).get();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("esAgentInfoDao insert error. Cause:{}", e.getMessage(), e);
		}
	}

	public void insert(AgentInfoBo agentInfoBo) throws JsonProcessingException {
		JSONObject jsonbject = BeanToJson.toEsTime(agentInfoBo);
		EsClient.client().prepareIndex(EsIndexs.AGENT_INFO, EsIndexs.TYPE)
				.setSource(jsonbject.toJSONString(), XContentType.JSON).get();
	}

}
