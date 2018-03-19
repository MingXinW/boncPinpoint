package com.navercorp.pinpoint.collector.dao.es;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.alibaba.fastjson.JSONObject;
import com.navercorp.pinpoint.collector.dao.MapStatisticsCalleeDao;
import com.navercorp.pinpoint.collector.dao.es.base.EsClient;
import com.navercorp.pinpoint.collector.dao.hbase.statistics.CallerColumnName;
import com.navercorp.pinpoint.collector.dao.hbase.statistics.ColumnName;
import com.navercorp.pinpoint.collector.util.BeanToJson;
import com.navercorp.pinpoint.collector.util.EsIndexs;
import com.navercorp.pinpoint.common.server.util.AcceptedTimeService;
import com.navercorp.pinpoint.common.trace.ServiceType;
import com.navercorp.pinpoint.common.util.ApplicationMapStatisticsUtils;

@Repository("esMapStatisticsCalleeDao")
public class ESMapStatisticsCalleeDao implements MapStatisticsCalleeDao{

	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@Autowired
    private AcceptedTimeService acceptedTimeService;
	
	@Override
	public void flushAll() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void update(String calleeApplicationName, ServiceType calleeServiceType, String callerApplicationName,
			ServiceType callerServiceType, String callerHost, int elapsed, boolean isError) {
		// TODO Auto-generated method stub
		if (callerApplicationName == null) {
            throw new NullPointerException("callerApplicationName must not be null");
        }
        if (calleeApplicationName == null) {
            throw new NullPointerException("calleeApplicationName must not be null");
        }

        if (logger.isDebugEnabled()) {
            logger.debug("[Callee] {} ({}) <- {} ({})[{}]",
                    calleeApplicationName, calleeServiceType, callerApplicationName, callerServiceType, callerHost);
        }
        try {
	        // there may be no endpoint in case of httpclient
	        callerHost = StringUtils.defaultString(callerHost);
	
	        // make row key. rowkey is me
	        final long acceptedTime = acceptedTimeService.getAcceptedTime();
	       
	
	        final short callerSlotNumber = ApplicationMapStatisticsUtils.getSlotNumber(calleeServiceType, elapsed, isError);
	        final ColumnName callerColumnName = new CallerColumnName(callerServiceType.getCode(), callerApplicationName, callerHost, callerSlotNumber);

	        String key = callerApplicationName + "^" +callerServiceType.getName()+"~"+calleeApplicationName+"^"+calleeServiceType.getName();
	        
	        JSONObject jsonbject = new JSONObject();
	        jsonbject.put("key", key);
			jsonbject.put("calleeServiceType", calleeServiceType.getCode());
			jsonbject.put("callerServiceType", callerServiceType.getCode());
			jsonbject.put("calleeApplicationName", calleeApplicationName);
			jsonbject.put("callerApplicationName", callerApplicationName);
			jsonbject.put("calleeHost", callerHost);
			jsonbject.put("calleeSlotNumber", callerSlotNumber);
			jsonbject.put("hashCode", callerColumnName.hashCode());
			jsonbject.put("acceptedTime", acceptedTime);
			jsonbject.put("elapsed", elapsed);
			jsonbject = BeanToJson.addEsTime(jsonbject);
			
			EsClient.client().prepareIndex(EsIndexs.APPLICATION_MAP_STATISTICS_CALLEE_VER2, EsIndexs.TYPE)
			.setSource(jsonbject.toJSONString(), XContentType.JSON).get();
        } catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("esMapStatisticsCalleeDao update error. Cause:{}", e.getMessage(), e);
		}

        
        
	}

}
