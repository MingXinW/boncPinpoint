package com.navercorp.pinpoint.collector.dao.proxy;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.SqlMetaDataDao;
import com.navercorp.pinpoint.thrift.dto.TSqlMetaData;

@Repository("sqlMetaDataDaoProxy")
public class SqlMetaDataDaoProxy implements SqlMetaDataDao {

	@Resource
	SqlMetaDataDao esSqlMetaDataDao;
	
	@Autowired(required = false)
	SqlMetaDataDao hbaseSqlMetaDataDao;
	
	@Override
	public void insert(TSqlMetaData sqlMetaData) {
		if(null != hbaseSqlMetaDataDao) {
			hbaseSqlMetaDataDao.insert(sqlMetaData);
		}
		esSqlMetaDataDao.insert(sqlMetaData);
	}

}
