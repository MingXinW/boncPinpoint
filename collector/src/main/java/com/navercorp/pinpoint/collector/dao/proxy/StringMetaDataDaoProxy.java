package com.navercorp.pinpoint.collector.dao.proxy;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.StringMetaDataDao;
import com.navercorp.pinpoint.thrift.dto.TStringMetaData;

@Repository("stringMetaDataDaoProxy")
public class StringMetaDataDaoProxy implements StringMetaDataDao {

	@Resource
	StringMetaDataDao esStringMetaDataDao;
	
	@Autowired(required = false)
	@Qualifier("hbaseStringMetaDataDao")
	StringMetaDataDao hbaseStringMetaDataDao;
	
	
	@Override
	public void insert(TStringMetaData stringMetaData) {
		if(null != hbaseStringMetaDataDao) {
			hbaseStringMetaDataDao.insert(stringMetaData);
		}
		esStringMetaDataDao.insert(stringMetaData);
	}

}
