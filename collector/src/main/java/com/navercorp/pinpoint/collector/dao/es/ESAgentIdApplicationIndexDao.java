package com.navercorp.pinpoint.collector.dao.es;

import com.navercorp.pinpoint.collector.dao.AgentIdApplicationIndexDao;

/**
 * @author futao
 *已经作废，故此不做es入库实现
 */
@Deprecated
public class ESAgentIdApplicationIndexDao implements AgentIdApplicationIndexDao {

	@Override
	public void insert(String agentId, String applicationName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String selectApplicationName(String agentId) {
		// TODO Auto-generated method stub
		return null;
	}

}
