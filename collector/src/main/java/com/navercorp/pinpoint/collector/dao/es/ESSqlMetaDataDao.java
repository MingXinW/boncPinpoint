package com.navercorp.pinpoint.collector.dao.es;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.navercorp.pinpoint.collector.dao.SqlMetaDataDao;
import com.navercorp.pinpoint.collector.dao.es.base.EsClient;
import com.navercorp.pinpoint.collector.util.EsIndexs;
import com.navercorp.pinpoint.thrift.dto.TSqlMetaData;

@Repository("esSqlMetaDataDao")
public class ESSqlMetaDataDao implements SqlMetaDataDao {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void insert(TSqlMetaData sqlMetaData) {
		if (sqlMetaData == null) {
			throw new NullPointerException("sqlMetaData must not be null");
		}
		if (logger.isDebugEnabled()) {
			logger.debug("insert:{}", sqlMetaData);
		}
		String id = sqlMetaData.getAgentId() + EsIndexs.ID_SEP + sqlMetaData.getAgentStartTime() + EsIndexs.ID_SEP
				+ sqlMetaData.getSqlId();
		try {
			ObjectMapper mapper = new ObjectMapper();
			byte[] json = mapper.writeValueAsBytes(sqlMetaData);
			EsClient.client().prepareIndex(EsIndexs.SQL_META_DATA_VER2, EsIndexs.TYPE, id).setSource(json).get();
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			logger.error("esSqlMetaDataDao insert error. Cause:{}", e.getMessage(), e);
		}
	}

}
