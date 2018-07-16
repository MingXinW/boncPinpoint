package com.navercorp.pinpoint.collector.dao.proxy;

import java.util.List;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.AgentStatDaoV2;
import com.navercorp.pinpoint.common.server.bo.stat.CpuLoadBo;

@Repository("cpuLoadBoProxy")
public class StatCpuLoadBoProxy implements AgentStatDaoV2<CpuLoadBo> {

	@Autowired(required = false)
	@Qualifier("hbaseCpuLoadDao")
	AgentStatDaoV2<CpuLoadBo> hbaseCpuLoadDao;
	
	@Resource
	AgentStatDaoV2<CpuLoadBo> esCpuLoadDao;
	
	@Override
	public void insert(String agentId, List<CpuLoadBo> agentStatDataPoints) {
		if(null != hbaseCpuLoadDao) {
			hbaseCpuLoadDao.insert(agentId, agentStatDataPoints);
		}
		esCpuLoadDao.insert(agentId, agentStatDataPoints);
	}

}
