package com.navercorp.pinpoint.collector.dao.proxy;

import javax.annotation.Resource;

import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.AgentStatDao;
import com.navercorp.pinpoint.thrift.dto.TAgentStat;

@Repository("agentStatDaoProxy")
public class StatAgentStatDaoProxy implements AgentStatDao {

	@Resource
	AgentStatDao esAgentStatDao;
	
	@Resource
	AgentStatDao hbaseAgentStatDao;
	
	@Override
	public void insert(TAgentStat agentStat) {
		// TODO Auto-generated method stub
		hbaseAgentStatDao.insert(agentStat);
		esAgentStatDao.insert(agentStat);
	}

}
