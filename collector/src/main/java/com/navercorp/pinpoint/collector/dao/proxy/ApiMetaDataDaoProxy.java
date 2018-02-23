package com.navercorp.pinpoint.collector.dao.proxy;

import javax.annotation.Resource;

import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.ApiMetaDataDao;
import com.navercorp.pinpoint.thrift.dto.TApiMetaData;

@Repository("apiMetaDataDaoProxy")
public class ApiMetaDataDaoProxy implements ApiMetaDataDao {

	@Resource
	ApiMetaDataDao hbaseApiMetaDataDao;
	
	@Resource
	ApiMetaDataDao esApiMetaDataDao;
	
	@Override
	public void insert(TApiMetaData apiMetaData) {
		// TODO Auto-generated method stub

		hbaseApiMetaDataDao.insert(apiMetaData);
		esApiMetaDataDao.insert(apiMetaData);
	}

}
