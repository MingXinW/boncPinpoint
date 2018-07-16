package com.navercorp.pinpoint.collector.dao.proxy;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.AgentInfoDao;
import com.navercorp.pinpoint.thrift.dto.TAgentInfo;

@Repository("agentInfoDaoProxy")
public class AgentInfoDaoProxy implements AgentInfoDao{

	@Resource
	AgentInfoDao esAgentInfoDao;
	
	@Autowired(required = false)
	@Qualifier("hbaseAgentInfoDao")
	AgentInfoDao hbaseAgentInfoDao;
	
	@Override
	public void insert(TAgentInfo agentInfo) {
		if(null != hbaseAgentInfoDao) {
			hbaseAgentInfoDao.insert(agentInfo);
		}
		esAgentInfoDao.insert(agentInfo);
	}

}
