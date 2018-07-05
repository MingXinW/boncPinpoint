package com.navercorp.pinpoint.collector.dao.proxy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.MapStatisticsCalleeDao;
import com.navercorp.pinpoint.common.trace.ServiceType;

@Repository("mapStatisticsCalleeDaoProxy")
public class MapStatisticsCalleeDaoProxy implements MapStatisticsCalleeDao{

	@Autowired(required = false)
	MapStatisticsCalleeDao hbaseMapStatisticsCalleeDao;
	
	@Autowired
	MapStatisticsCalleeDao esMapStatisticsCalleeDao;
	
	@Override
	public void flushAll() {
		if(null != hbaseMapStatisticsCalleeDao) {
			hbaseMapStatisticsCalleeDao.flushAll();
		}
		esMapStatisticsCalleeDao.flushAll();
	}

	@Override
	public void update(String calleeApplicationName, ServiceType calleeServiceType, String callerApplicationName,
			ServiceType callerServiceType, String callerHost, int elapsed, boolean isError) {
		if(null != hbaseMapStatisticsCalleeDao) {
			hbaseMapStatisticsCalleeDao.update(calleeApplicationName, calleeServiceType, callerApplicationName, callerServiceType, callerHost, elapsed, isError);
		}
		esMapStatisticsCalleeDao.update(calleeApplicationName, calleeServiceType, callerApplicationName, callerServiceType, callerHost, elapsed, isError);
	}

}
