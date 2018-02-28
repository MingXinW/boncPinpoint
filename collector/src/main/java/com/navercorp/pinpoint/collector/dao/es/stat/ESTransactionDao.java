package com.navercorp.pinpoint.collector.dao.es.stat;

import java.util.List;

import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.AgentStatDaoV2;
import com.navercorp.pinpoint.collector.dao.es.base.AgentStatESOperationFactory;
import com.navercorp.pinpoint.common.server.bo.stat.AgentStatType;
import com.navercorp.pinpoint.common.server.bo.stat.TransactionBo;

@Repository("esTransactionDao")
public class ESTransactionDao implements AgentStatDaoV2<TransactionBo> {

	@Override
	public void insert(String agentId, List<TransactionBo> transactionBos) {
		// TODO Auto-generated method stub
		if (agentId == null) {
			throw new NullPointerException("agentId must not be null");
		}
		if (transactionBos == null || transactionBos.isEmpty()) {
			return;
		}
		AgentStatESOperationFactory.createPuts(agentId, AgentStatType.ACTIVE_TRACE, transactionBos);
	}

}
