package com.navercorp.pinpoint.collector.dao.es;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.alibaba.fastjson.JSONObject;
import com.navercorp.pinpoint.collector.dao.ApplicationTraceIndexDao;
import com.navercorp.pinpoint.collector.dao.es.base.EsClient;
import com.navercorp.pinpoint.collector.util.BeanToJson;
import com.navercorp.pinpoint.collector.util.EsIndexs;
import com.navercorp.pinpoint.common.server.util.AcceptedTimeService;
import com.navercorp.pinpoint.common.util.TransactionId;
import com.navercorp.pinpoint.common.util.TransactionIdUtils;
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
		//String applicationName = span.getApplicationName();
		final byte[] transactionIdBytes = span.getTransactionId();
        TransactionId transactionId = TransactionIdUtils.parseTransactionId(transactionIdBytes);
		//String id = applicationName + EsIndexs.ID_SEP + acceptedTime;
		
		try {
			JSONObject jsonbject = new JSONObject();
			jsonbject.put("agentId", span.getAgentId());
			jsonbject.put("applicationName", span.getApplicationName());
			jsonbject.put("acceptedTime", acceptedTime);
			jsonbject.put("transactionId", transactionId);
			jsonbject = BeanToJson.addEsTime(jsonbject);
			/*EsClient.client().prepareIndex(EsIndexs.APPLICATION_TRACE_INDEX, EsIndexs.TYPE, id)
			.setSource(jsonbject.toJSONString(),XContentType.JSON).get();*/
			
			EsClient.insert(jsonbject,EsIndexs.APPLICATION_TRACE_INDEX, EsIndexs.TYPE);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("esApplicationTraceIndexDao insert error. Cause:{}", e.getMessage(), e);
		}
	}

}
