package com.navercorp.pinpoint.collector.dao.proxy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.MapResponseTimeDao;
import com.navercorp.pinpoint.common.trace.ServiceType;

@Repository("mapResponseTimeDaoProxy")
public class MapResponseTimeDaoProxy implements MapResponseTimeDao {

	@Autowired
	MapResponseTimeDao hbaseMapResponseTimeDao;
	
	@Autowired
	MapResponseTimeDao esMapResponseTimeDao;
	
	@Override
	public void flushAll() {
		// TODO Auto-generated method stub

		hbaseMapResponseTimeDao.flushAll();
		esMapResponseTimeDao.flushAll();
	}

	@Override
	public void received(String applicationName, ServiceType serviceType, String agentId, int elapsed,
			boolean isError) {
		// TODO Auto-generated method stub

		hbaseMapResponseTimeDao.received(applicationName, serviceType, agentId, elapsed, isError);
		esMapResponseTimeDao.received(applicationName, serviceType, agentId, elapsed, isError);
	}

}
