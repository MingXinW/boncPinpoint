package com.navercorp.pinpoint.collector.dao.proxy;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.AgentEventDao;
import com.navercorp.pinpoint.collector.dao.es.ESAgentEventDao;
import com.navercorp.pinpoint.collector.dao.hbase.HbaseAgentEventDao;
import com.navercorp.pinpoint.common.server.bo.event.AgentEventBo;

@Repository("agentEventDaoProxy")
public class AgentEventDaoProxy implements AgentEventDao {

	@Autowired(required = false)
	private HbaseAgentEventDao hbaseAgentEventDao;

	@Resource
	private ESAgentEventDao esAgentEventDao;

	@Override
	public void insert(AgentEventBo agentEventBo) {
		if(null != hbaseAgentEventDao) {
			hbaseAgentEventDao.insert(agentEventBo);
		}
		esAgentEventDao.insert(agentEventBo);
	}

}
