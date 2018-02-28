package com.navercorp.pinpoint.collector.dao.es.stat;

import java.util.List;

import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.AgentStatDaoV2;
import com.navercorp.pinpoint.collector.dao.es.base.AgentStatESOperationFactory;
import com.navercorp.pinpoint.common.server.bo.stat.AgentStatType;
import com.navercorp.pinpoint.common.server.bo.stat.CpuLoadBo;

@Repository("esCpuLoadDao")
public class ESCpuLoadDao implements AgentStatDaoV2<CpuLoadBo> {

	@Override
	public void insert(String agentId, List<CpuLoadBo> cpuLoadBos) {
		// TODO Auto-generated method stub
		if (agentId == null) {
			throw new NullPointerException("agentId must not be null");
		}
		if (cpuLoadBos == null || cpuLoadBos.isEmpty()) {
			return;
		}
		AgentStatESOperationFactory.createPuts( agentId, AgentStatType.CPU_LOAD, cpuLoadBos);
	}

}
