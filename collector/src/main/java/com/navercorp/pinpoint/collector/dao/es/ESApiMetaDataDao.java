package com.navercorp.pinpoint.collector.dao.es;

import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.navercorp.pinpoint.collector.dao.ApiMetaDataDao;
import com.navercorp.pinpoint.collector.dao.es.base.EsClient;
import com.navercorp.pinpoint.collector.util.BeanToJson;
import com.navercorp.pinpoint.collector.util.EsIndexs;
import com.navercorp.pinpoint.common.server.bo.ApiMetaDataBo;
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

		apiMetaDataBo.setApiInfo(apiMetaData.getApiInfo());
		if (apiMetaData.isSetLine()) {
			apiMetaDataBo.setLineNumber(apiMetaData.getLine());
		} else {
			apiMetaDataBo.setLineNumber(-1);
		}

		String id = apiMetaDataBo.getAgentId() + EsIndexs.ID_SEP + apiMetaDataBo.getStartTime() + EsIndexs.ID_SEP + apiMetaDataBo.getApiId();

		try {
			JSONObject jsonbject = BeanToJson.toEsTime(apiMetaDataBo);
			
			if (apiMetaData.isSetType()) {
				/*jsonbject.put("", MethodTypeEnum.valueOf(apiMetaData.getType()));*/
				jsonbject.put("type", apiMetaData.getType());
			} else {
				jsonbject.put("type", 0);
			}
			
			EsClient.client().prepareIndex(EsIndexs.API_METADATA, EsIndexs.TYPE,id)
					.setSource(jsonbject.toJSONString(), XContentType.JSON).get();
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			logger.error("esApiMetaDataDao insert error. Cause:{}", e.getMessage(), e);
		}
	}

}
