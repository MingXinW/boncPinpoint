package com.navercorp.pinpoint.collector.dao.proxy;

import javax.annotation.Resource;

import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.HostApplicationMapDao;

@Repository("hostApplicationMapDaoProxy")
public class HostApplicationMapDaoProxy implements HostApplicationMapDao {

	@Resource
	HostApplicationMapDao esHostApplicationMapDao;
	
	@Resource
	HostApplicationMapDao hbaseHostApplicationMapDao;
	
	@Override
	public void insert(String host, String bindApplicationName, short bindServiceType, String parentApplicationName,
			short parentServiceType) {
		// TODO Auto-generated method stub

		hbaseHostApplicationMapDao.insert(host, bindApplicationName, bindServiceType, parentApplicationName, parentServiceType);
		esHostApplicationMapDao.insert(host, bindApplicationName, bindServiceType, parentApplicationName, parentServiceType);
	}

}
