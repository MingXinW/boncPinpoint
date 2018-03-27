package com.navercorp.pinpoint.collector.dao.proxy;

import javax.annotation.Resource;

import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.AgentInfoDao;
import com.navercorp.pinpoint.thrift.dto.TAgentInfo;

@Repository("agentInfoDaoProxy")
public class AgentInfoDaoProxy implements AgentInfoDao{

	@Resource
	AgentInfoDao esAgentInfoDao;
	
	@Resource
	AgentInfoDao hbaseAgentInfoDao;
	
	@Override
	public void insert(TAgentInfo agentInfo) {
		// TODO Auto-generated method stub
		hbaseAgentInfoDao.insert(agentInfo);
		esAgentInfoDao.insert(agentInfo);
	}

}
