package com.navercorp.pinpoint.collector.dao.proxy;

import javax.annotation.Resource;

import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.AgentEventDao;
import com.navercorp.pinpoint.collector.dao.es.ESAgentEventDao;
import com.navercorp.pinpoint.collector.dao.hbase.HbaseAgentEventDao;
import com.navercorp.pinpoint.common.server.bo.AgentEventBo;

@Repository("agentEventDaoProxy")
public class AgentEventDaoProxy implements AgentEventDao {

	@Resource
	private HbaseAgentEventDao hbaseAgentEventDao;

	@Resource
	private ESAgentEventDao eSAgentEventDao;

	@Override
	public void insert(AgentEventBo agentEventBo) {
		hbaseAgentEventDao.insert(agentEventBo);
		eSAgentEventDao.insert(agentEventBo);
	}

}
