package com.navercorp.pinpoint.collector.dao.proxy;

import javax.annotation.Resource;

import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.AgentLifeCycleDao;
import com.navercorp.pinpoint.common.server.bo.AgentLifeCycleBo;

@Repository("agentLifeCycleDaoProxy")
public class AgentLifeCycleDaoProxy implements AgentLifeCycleDao {

	@Resource
	AgentLifeCycleDao esAgentLifeCycleDao;
	
	@Resource
	AgentLifeCycleDao hbaseAgentLifeCycleDao;
	
	@Override
	public void insert(AgentLifeCycleBo agentLifeCycleBo) {
		// TODO Auto-generated method stub
		hbaseAgentLifeCycleDao.insert(agentLifeCycleBo);
		esAgentLifeCycleDao.insert(agentLifeCycleBo);
	}

}
