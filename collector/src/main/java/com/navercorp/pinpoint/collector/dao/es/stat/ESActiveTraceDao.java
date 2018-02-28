package com.navercorp.pinpoint.collector.dao.es.stat;

import java.util.List;

import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.AgentStatDaoV2;
import com.navercorp.pinpoint.collector.dao.es.base.AgentStatESOperationFactory;
import com.navercorp.pinpoint.common.server.bo.stat.ActiveTraceBo;
import com.navercorp.pinpoint.common.server.bo.stat.AgentStatType;

@Repository("esActiveTraceDao")
public class ESActiveTraceDao implements AgentStatDaoV2<ActiveTraceBo> {

	
	@Override
	public void insert(String agentId, List<ActiveTraceBo> agentStatDataPoints) {
		if (agentId == null) {
            throw new NullPointerException("agentId must not be null");
        }
        if (agentStatDataPoints == null || agentStatDataPoints.isEmpty()) {
            return;
        }
		AgentStatESOperationFactory.createPuts( agentId, AgentStatType.ACTIVE_TRACE, agentStatDataPoints);
		
	}

}
