package com.navercorp.pinpoint.collector.dao.proxy;

import java.util.List;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.AgentStatDaoV2;
import com.navercorp.pinpoint.common.server.bo.stat.JvmGcBo;

@Repository("jvmGcBoProxy")
public class StatJvmGcBoProxy implements AgentStatDaoV2<JvmGcBo> {

	@Autowired(required = false)
	AgentStatDaoV2<JvmGcBo> hbaseJvmGcDao;
	
	@Resource
	AgentStatDaoV2<JvmGcBo> esJvmGcDao;
	
	@Override
	public void insert(String agentId, List<JvmGcBo> agentStatDataPoints) {
		if(null != hbaseJvmGcDao) {
			hbaseJvmGcDao.insert(agentId, agentStatDataPoints);
		}
		esJvmGcDao.insert(agentId, agentStatDataPoints);
	}

}
