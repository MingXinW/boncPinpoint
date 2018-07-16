package com.navercorp.pinpoint.collector.dao.proxy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.MapResponseTimeDao;
import com.navercorp.pinpoint.common.trace.ServiceType;

@Repository("mapResponseTimeDaoProxy")
public class MapResponseTimeDaoProxy implements MapResponseTimeDao {

	@Autowired(required = false)
	@Qualifier("hbaseMapResponseTimeDao")
	MapResponseTimeDao hbaseMapResponseTimeDao;
	
	@Autowired
	MapResponseTimeDao esMapResponseTimeDao;
	
	@Override
	public void flushAll() {
		if(null != hbaseMapResponseTimeDao) {
			hbaseMapResponseTimeDao.flushAll();
		}
		esMapResponseTimeDao.flushAll();
	}

	@Override
	public void received(String applicationName, ServiceType serviceType, String agentId, int elapsed,
			boolean isError) {
		if(null != hbaseMapResponseTimeDao) {
			hbaseMapResponseTimeDao.received(applicationName, serviceType, agentId, elapsed, isError);
		}
		esMapResponseTimeDao.received(applicationName, serviceType, agentId, elapsed, isError);
	}

}
