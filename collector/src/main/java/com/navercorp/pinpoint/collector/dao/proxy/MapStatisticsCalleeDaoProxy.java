package com.navercorp.pinpoint.collector.dao.proxy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.MapStatisticsCalleeDao;
import com.navercorp.pinpoint.common.trace.ServiceType;

@Repository("mapStatisticsCalleeDaoProxy")
public class MapStatisticsCalleeDaoProxy implements MapStatisticsCalleeDao{

	@Autowired
	MapStatisticsCalleeDao hbaseMapStatisticsCalleeDao;
	
	@Autowired
	MapStatisticsCalleeDao esMapStatisticsCalleeDao;
	
	@Override
	public void flushAll() {
		// TODO Auto-generated method stub
		hbaseMapStatisticsCalleeDao.flushAll();
		esMapStatisticsCalleeDao.flushAll();
	}

	@Override
	public void update(String calleeApplicationName, ServiceType calleeServiceType, String callerApplicationName,
			ServiceType callerServiceType, String callerHost, int elapsed, boolean isError) {
		// TODO Auto-generated method stub
		hbaseMapStatisticsCalleeDao.update(calleeApplicationName, calleeServiceType, callerApplicationName, callerServiceType, callerHost, elapsed, isError);
		esMapStatisticsCalleeDao.update(calleeApplicationName, calleeServiceType, callerApplicationName, callerServiceType, callerHost, elapsed, isError);
	}

}
