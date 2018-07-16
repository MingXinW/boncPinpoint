package com.navercorp.pinpoint.collector.dao.proxy;

import java.util.List;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.AgentStatDaoV2;
import com.navercorp.pinpoint.common.server.bo.stat.JvmGcDetailedBo;

@Repository("jvmGcDetailedBoProxy")
public class StatJvmGcDetailedBoProxy implements AgentStatDaoV2<JvmGcDetailedBo> {
	
	@Autowired(required = false)
	@Qualifier("hbaseJvmGcDetailedDao")
	AgentStatDaoV2<JvmGcDetailedBo> hbaseJvmGcDetailedDao;
	
	@Resource
	AgentStatDaoV2<JvmGcDetailedBo> esJvmGcDetailedDao;
	
	@Override
	public void insert(String agentId, List<JvmGcDetailedBo> agentStatDataPoints) {
		if(null != hbaseJvmGcDetailedDao) {
			hbaseJvmGcDetailedDao.insert(agentId, agentStatDataPoints);
		}
		esJvmGcDetailedDao.insert(agentId, agentStatDataPoints);
	}

}
