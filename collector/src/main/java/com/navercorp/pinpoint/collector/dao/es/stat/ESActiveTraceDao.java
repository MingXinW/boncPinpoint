package com.navercorp.pinpoint.collector.dao.es.stat;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.AgentStatDaoV2;
import com.navercorp.pinpoint.collector.dao.es.base.AgentStatESOperationFactory;
import com.navercorp.pinpoint.common.server.bo.stat.ActiveTraceBo;
import com.navercorp.pinpoint.common.server.bo.stat.AgentStatType;

@Repository("esActiveTraceDao")
public class ESActiveTraceDao implements AgentStatDaoV2<ActiveTraceBo> {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@Override
	public void insert(String agentId, List<ActiveTraceBo> agentStatDataPoints) {
		if (agentId == null) {
            throw new NullPointerException("agentId must not be null");
        }
        if (agentStatDataPoints == null || agentStatDataPoints.isEmpty()) {
            return;
        }
		try {
			AgentStatESOperationFactory.createPuts( agentId, AgentStatType.ACTIVE_TRACE, agentStatDataPoints);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("esActiveTraceDao insert error. Cause:{}", e.getMessage(), e);
		}
		
	}

}
