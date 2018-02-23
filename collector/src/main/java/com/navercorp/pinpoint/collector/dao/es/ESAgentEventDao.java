package com.navercorp.pinpoint.collector.dao.es;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.navercorp.pinpoint.collector.dao.AgentEventDao;
import com.navercorp.pinpoint.collector.dao.es.base.EsClient;
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
			ObjectMapper mapper = new ObjectMapper();
			byte[] json = mapper.writeValueAsBytes(agentEventBo);
			EsClient.client().prepareIndex(EsIndexs.AGENT_EVENT, EsIndexs.TYPE, id)
					.setSource(json).get();
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
