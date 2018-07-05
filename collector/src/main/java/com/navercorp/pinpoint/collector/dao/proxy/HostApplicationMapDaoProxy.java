package com.navercorp.pinpoint.collector.dao.proxy;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.HostApplicationMapDao;

@Repository("hostApplicationMapDaoProxy")
public class HostApplicationMapDaoProxy implements HostApplicationMapDao {

	@Resource
	HostApplicationMapDao esHostApplicationMapDao;
	
	@Autowired(required = false)
	HostApplicationMapDao hbaseHostApplicationMapDao;
	
	@Override
	public void insert(String host, String bindApplicationName, short bindServiceType, String parentApplicationName,
			short parentServiceType) {
		if(null != hbaseHostApplicationMapDao) {
			hbaseHostApplicationMapDao.insert(host, bindApplicationName, bindServiceType, parentApplicationName, parentServiceType);
		}
		esHostApplicationMapDao.insert(host, bindApplicationName, bindServiceType, parentApplicationName, parentServiceType);
	}

}
