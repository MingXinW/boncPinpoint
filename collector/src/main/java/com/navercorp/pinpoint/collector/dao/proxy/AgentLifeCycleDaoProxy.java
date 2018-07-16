package com.navercorp.pinpoint.collector.dao.proxy;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.AgentLifeCycleDao;
import com.navercorp.pinpoint.common.server.bo.AgentLifeCycleBo;

@Repository("agentLifeCycleDaoProxy")
public class AgentLifeCycleDaoProxy implements AgentLifeCycleDao {

	@Resource
	AgentLifeCycleDao esAgentLifeCycleDao;
	
	@Autowired(required = false)
	@Qualifier("hbaseAgentLifeCycleDao")
	AgentLifeCycleDao hbaseAgentLifeCycleDao;
	
	@Override
	public void insert(AgentLifeCycleBo agentLifeCycleBo) {
		if(null != hbaseAgentLifeCycleDao) {
			hbaseAgentLifeCycleDao.insert(agentLifeCycleBo);
		}
		esAgentLifeCycleDao.insert(agentLifeCycleBo);
	}

}
