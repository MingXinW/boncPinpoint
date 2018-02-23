package com.navercorp.pinpoint.collector.dao.proxy;

import javax.annotation.Resource;

import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.StringMetaDataDao;
import com.navercorp.pinpoint.thrift.dto.TStringMetaData;

@Repository("stringMetaDataDaoProxy")
public class StringMetaDataDaoProxy implements StringMetaDataDao {

	@Resource
	StringMetaDataDao esStringMetaDataDao;
	
	@Resource
	StringMetaDataDao hbaseStringMetaDataDao;
	
	
	@Override
	public void insert(TStringMetaData stringMetaData) {
		// TODO Auto-generated method stub

		hbaseStringMetaDataDao.insert(stringMetaData);
		esStringMetaDataDao.insert(stringMetaData);
	}

}
