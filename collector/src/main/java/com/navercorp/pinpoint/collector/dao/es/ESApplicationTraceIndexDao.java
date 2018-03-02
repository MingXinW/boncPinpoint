package com.navercorp.pinpoint.collector.dao.es;


import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.ApplicationTraceIndexDao;
import com.navercorp.pinpoint.collector.dao.es.base.EsClient;
import com.navercorp.pinpoint.collector.util.EsIndexs;
import com.navercorp.pinpoint.common.server.util.AcceptedTimeService;
import com.navercorp.pinpoint.thrift.dto.TSpan;

@Repository("esApplicationTraceIndexDao")
public class ESApplicationTraceIndexDao implements ApplicationTraceIndexDao {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@Autowired
    private AcceptedTimeService acceptedTimeService;
	
	@Override
	public void insert(TSpan span) {
		// TODO Auto-generated method stub

		if (span == null) {
            throw new NullPointerException("span must not be null");
        }
		
		long acceptedTime = acceptedTimeService.getAcceptedTime();
		String applicationName = span.getApplicationName();
		String id = acceptedTime + EsIndexs.ID_SEP + applicationName;
		
		try {
			EsClient.client().prepareIndex(EsIndexs.APPLICATION_TRACE_INDEX, EsIndexs.TYPE,id).setSource(
					jsonBuilder()
			        .startObject()
			        .field("elapsed", span.getElapsed())
			        .field("err", span.getErr())
			        .field("agentId", span.getAgentId())
			    .endObject()).get();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("esApplicationTraceIndexDao insert error. Cause:{}", e.getMessage(), e);
		}
	}

}
