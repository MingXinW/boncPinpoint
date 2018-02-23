package com.navercorp.pinpoint.collector.dao.es;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.navercorp.pinpoint.collector.dao.SqlMetaDataDao;
import com.navercorp.pinpoint.thrift.dto.TSqlMetaData;

public class ESSqlMetaDataDao implements SqlMetaDataDao {

	 private final Logger logger = LoggerFactory.getLogger(this.getClass());
	 
	@Override
	public void insert(TSqlMetaData sqlMetaData) {
		// TODO Auto-generated method stub

	}

}
