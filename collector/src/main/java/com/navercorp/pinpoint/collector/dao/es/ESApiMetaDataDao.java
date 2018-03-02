package com.navercorp.pinpoint.collector.dao.es;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.navercorp.pinpoint.collector.dao.ApiMetaDataDao;
import com.navercorp.pinpoint.collector.dao.es.base.EsClient;
import com.navercorp.pinpoint.collector.util.EsIndexs;
import com.navercorp.pinpoint.common.server.bo.ApiMetaDataBo;
import com.navercorp.pinpoint.common.server.bo.MethodTypeEnum;
import com.navercorp.pinpoint.thrift.dto.TApiMetaData;

@Repository("esApiMetaDataDao")
public class ESApiMetaDataDao implements ApiMetaDataDao {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void insert(TApiMetaData apiMetaData) {
		// TODO Auto-generated method stub

		if (logger.isDebugEnabled()) {
			logger.debug("insert:{}", apiMetaData);
		}

		ApiMetaDataBo apiMetaDataBo = new ApiMetaDataBo(apiMetaData.getAgentId(), apiMetaData.getAgentStartTime(),
				apiMetaData.getApiId());

		String agentId = apiMetaDataBo.getAgentId();
		long startTime = apiMetaDataBo.getStartTime();
		int apiId = apiMetaDataBo.getApiId();
		String id = agentId + EsIndexs.ID_SEP + startTime + EsIndexs.ID_SEP + apiId;
		apiMetaDataBo.setApiInfo(apiMetaData.getApiInfo());
		apiMetaDataBo.setLineNumber(apiMetaData.getLine());
		apiMetaDataBo.setMethodTypeEnum(MethodTypeEnum.valueOf(apiMetaData.getType()));

		try {
			ObjectMapper mapper = new ObjectMapper();
			byte[] json = mapper.writeValueAsBytes(apiMetaDataBo);
			EsClient.client().prepareIndex(EsIndexs.API_METADATA, EsIndexs.TYPE, id)
					.setSource(json).get();
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			logger.error("esApiMetaDataDao insert error. Cause:{}", e.getMessage(), e);
		}
	}

}
