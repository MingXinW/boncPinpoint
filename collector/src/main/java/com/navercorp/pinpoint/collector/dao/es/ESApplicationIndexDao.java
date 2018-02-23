package com.navercorp.pinpoint.collector.dao.es;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.ApplicationIndexDao;
import com.navercorp.pinpoint.collector.dao.es.base.EsClient;
import com.navercorp.pinpoint.collector.util.EsIndexs;
import com.navercorp.pinpoint.thrift.dto.TAgentInfo;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

import java.io.IOException;

@Repository("esApplicationIndexDao")
public class ESApplicationIndexDao implements ApplicationIndexDao {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Override
	public void insert(TAgentInfo agentInfo) {
		// TODO Auto-generated method stub

		if (agentInfo == null) {
			throw new NullPointerException("agentInfo must not be null");
		}
		
		
		String id = agentInfo.getApplicationName();
		
		try {
			EsClient.client().prepareIndex(EsIndexs.APPLICATION_INDEX, EsIndexs.TYPE,id).setSource(
					jsonBuilder()
			        .startObject()
			        .field("agentId", agentInfo.getAgentId())
			        .field("serviceType", agentInfo.getServiceType())
			    .endObject()).get();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		logger.debug("Insert agentInfo. {}", agentInfo);
	}

}
