package com.navercorp.pinpoint.collector.dao.proxy;

import java.util.List;

import javax.annotation.Resource;

import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.AgentStatDaoV2;
import com.navercorp.pinpoint.common.server.bo.stat.JvmGcDetailedBo;

@Repository("jvmGcDetailedBoProxy")
public class StatJvmGcDetailedBoProxy implements AgentStatDaoV2<JvmGcDetailedBo> {
	
	@Resource
	AgentStatDaoV2<JvmGcDetailedBo> hbaseJvmGcDetailedDao;
	
	@Resource
	AgentStatDaoV2<JvmGcDetailedBo> esJvmGcDetailedDao;
	
	@Override
	public void insert(String agentId, List<JvmGcDetailedBo> agentStatDataPoints) {
		// TODO Auto-generated method stub
		hbaseJvmGcDetailedDao.insert(agentId, agentStatDataPoints);
		esJvmGcDetailedDao.insert(agentId, agentStatDataPoints);
	}

}
