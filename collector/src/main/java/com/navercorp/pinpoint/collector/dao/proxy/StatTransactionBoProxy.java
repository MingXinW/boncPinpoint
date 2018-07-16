package com.navercorp.pinpoint.collector.dao.proxy;

import java.util.List;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.AgentStatDaoV2;
import com.navercorp.pinpoint.common.server.bo.stat.TransactionBo;


@Repository("transactionBoProxy")
public class StatTransactionBoProxy implements AgentStatDaoV2<TransactionBo> {

	@Autowired(required = false)
	@Qualifier("hbaseTransactionDao")
	AgentStatDaoV2<TransactionBo> hbaseTransactionDao;
	
	@Resource
	AgentStatDaoV2<TransactionBo> esTransactionDao;
	
	@Override
	public void insert(String agentId, List<TransactionBo> agentStatDataPoints) {
		if(null != hbaseTransactionDao) {
			hbaseTransactionDao.insert(agentId, agentStatDataPoints);
		}
		esTransactionDao.insert(agentId, agentStatDataPoints);
	}

}
