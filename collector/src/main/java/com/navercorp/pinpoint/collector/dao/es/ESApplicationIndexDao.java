package com.navercorp.pinpoint.collector.dao.es;

import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.alibaba.fastjson.JSONObject;
import com.navercorp.pinpoint.collector.dao.ApplicationIndexDao;
import com.navercorp.pinpoint.collector.dao.es.base.EsClient;
import com.navercorp.pinpoint.collector.util.BeanToJson;
import com.navercorp.pinpoint.collector.util.EsIndexs;
import com.navercorp.pinpoint.common.service.ServiceTypeRegistryService;
import com.navercorp.pinpoint.common.trace.ServiceType;
import com.navercorp.pinpoint.thrift.dto.TAgentInfo;


@Repository("esApplicationIndexDao")
public class ESApplicationIndexDao implements ApplicationIndexDao {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Autowired
    private ServiceTypeRegistryService registry;
	
	@Override
	public void insert(TAgentInfo agentInfo) {

		if (agentInfo == null) {
			throw new NullPointerException("agentInfo must not be null");
		}
		String id = agentInfo.getApplicationName();
		try {
			ServiceType serviceType = registry.findServiceType(agentInfo.getServiceType());
			JSONObject jsonbject = new JSONObject();
			jsonbject.put("agentId", agentInfo.getAgentId());
			jsonbject.put("applicationName", agentInfo.getApplicationName());
			jsonbject.put("code", agentInfo.getServiceType());
			jsonbject.put("serviceType", serviceType.getName());
			jsonbject = BeanToJson.addEsTime(jsonbject);
			EsClient.client().prepareIndex(EsIndexs.APPLICATION_INDEX, EsIndexs.TYPE, id)
			.setSource(jsonbject.toJSONString(),XContentType.JSON).get();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("esApplicationIndexDao insert error. Cause:{}", e.getMessage(), e);
		}
	}

}
