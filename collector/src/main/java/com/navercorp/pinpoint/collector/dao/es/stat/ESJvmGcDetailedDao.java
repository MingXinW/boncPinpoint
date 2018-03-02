package com.navercorp.pinpoint.collector.dao.es.stat;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.AgentStatDaoV2;
import com.navercorp.pinpoint.collector.dao.es.base.AgentStatESOperationFactory;
import com.navercorp.pinpoint.common.server.bo.stat.AgentStatType;
import com.navercorp.pinpoint.common.server.bo.stat.JvmGcDetailedBo;

@Repository("esJvmGcDetailedDao")
public class ESJvmGcDetailedDao implements AgentStatDaoV2<JvmGcDetailedBo> {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@Override
	public void insert(String agentId, List<JvmGcDetailedBo> jvmGcDetailedBos) {
		// TODO Auto-generated method stub
		if (agentId == null) {
            throw new NullPointerException("agentId must not be null");
        }
        if (jvmGcDetailedBos == null || jvmGcDetailedBos.isEmpty()) {
            return;
        }
        
        try {
			AgentStatESOperationFactory.createPuts( agentId, AgentStatType.JVM_GC_DETAILED, jvmGcDetailedBos);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("esJvmGcDetailedDao insert error. Cause:{}", e.getMessage(), e);
		}
	}

}
