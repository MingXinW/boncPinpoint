package com.navercorp.pinpoint.collector.dao.es.stat;

import java.util.List;

import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.AgentStatDaoV2;
import com.navercorp.pinpoint.collector.dao.es.base.AgentStatESOperationFactory;
import com.navercorp.pinpoint.common.server.bo.stat.AgentStatType;
import com.navercorp.pinpoint.common.server.bo.stat.JvmGcBo;

@Repository("esJvmGcDao")
public class ESJvmGcDao implements AgentStatDaoV2<JvmGcBo> {

	@Override
	public void insert(String agentId, List<JvmGcBo> jvmGcBos) {
		// TODO Auto-generated method stub
		if (agentId == null) {
			throw new NullPointerException("agentId must not be null");
		}
		if (jvmGcBos == null || jvmGcBos.isEmpty()) {
			return;
		}
		AgentStatESOperationFactory.createPuts(agentId, AgentStatType.JVM_GC, jvmGcBos);
	}

}
