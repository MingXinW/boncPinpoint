package com.navercorp.pinpoint.collector.dao.proxy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.MapStatisticsCallerDao;
import com.navercorp.pinpoint.common.trace.ServiceType;

@Repository("mapStatisticsCallerDaoProxy")
public class MapStatisticsCallerDaoProxy implements MapStatisticsCallerDao {

	@Autowired(required = false)
	private MapStatisticsCallerDao hbaseMapStatisticsCallerDao;

	@Autowired
	private MapStatisticsCallerDao esMapStatisticsCallerDao;

	@Override
	public void flushAll() {
		if(null != hbaseMapStatisticsCallerDao) {
			hbaseMapStatisticsCallerDao.flushAll();
		}
		esMapStatisticsCallerDao.flushAll();
	}

	@Override
	public void update(String callerApplicationName, ServiceType callerServiceType, String callerAgentId,
			String calleeApplicationName, ServiceType calleeServiceType, String calleeHost, int elapsed,
			boolean isError) {
		if(null != hbaseMapStatisticsCallerDao) {
			hbaseMapStatisticsCallerDao.update(callerApplicationName, callerServiceType, callerAgentId, calleeApplicationName, calleeServiceType, calleeHost, elapsed, isError);
		}
		esMapStatisticsCallerDao.update(callerApplicationName, callerServiceType, callerAgentId, calleeApplicationName, calleeServiceType, calleeHost, elapsed, isError);
	}

}
