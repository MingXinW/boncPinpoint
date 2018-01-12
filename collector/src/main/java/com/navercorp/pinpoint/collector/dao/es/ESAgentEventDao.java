package com.navercorp.pinpoint.collector.dao.es;

import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.AgentEventDao;
import com.navercorp.pinpoint.common.server.bo.AgentEventBo;

@Repository
public class ESAgentEventDao implements AgentEventDao {

	@Override
	public void insert(AgentEventBo agentEventBo) {
		System.out.println("es insert impl .." + agentEventBo.toString());

	}

}
 