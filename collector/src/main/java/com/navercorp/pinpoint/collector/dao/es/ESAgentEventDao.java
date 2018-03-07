package com.navercorp.pinpoint.collector.dao.es;

import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.navercorp.pinpoint.collector.dao.AgentEventDao;
import com.navercorp.pinpoint.collector.dao.es.base.EsClient;
import com.navercorp.pinpoint.collector.util.BeanToJson;
import com.navercorp.pinpoint.collector.util.EsIndexs;
import com.navercorp.pinpoint.common.server.bo.AgentEventBo;
import com.navercorp.pinpoint.common.util.TimeUtils;

@Repository("esAgentEventDao")
public class ESAgentEventDao implements AgentEventDao {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void insert(AgentEventBo agentEventBo) {
		if (agentEventBo == null) {
			throw new NullPointerException("agentEventBo must not be null");
		}

		if (logger.isDebugEnabled()) {
			logger.debug("insert event. {}", agentEventBo.toString());
		}

		final String agentId = agentEventBo.getAgentId();
		final long eventTimestamp = agentEventBo.getEventTimestamp();
		long reverseStartTimestamp = TimeUtils.reverseTimeMillis(eventTimestamp);
		String id = agentId + EsIndexs.ID_SEP + reverseStartTimestamp;

		try {
			/*EsClient.insert(agentEventBo,id, EsIndexs.AGENT_EVENT, EsIndexs.TYPE);*/
			JSONObject jsonbject = BeanToJson.toEsTime(agentEventBo);
			EsClient.client().prepareIndex(EsIndexs.AGENT_EVENT, EsIndexs.TYPE, id)
			.setSource(jsonbject.toJSONString(),XContentType.JSON).get();
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			logger.error("esAgentEventDao insert error. Cause:{}", e.getMessage(), e);
		}
	}

}
