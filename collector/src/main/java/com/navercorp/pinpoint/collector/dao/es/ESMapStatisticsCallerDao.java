package com.navercorp.pinpoint.collector.dao.es;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.alibaba.fastjson.JSONObject;
import com.navercorp.pinpoint.collector.dao.MapStatisticsCallerDao;
import com.navercorp.pinpoint.collector.dao.es.base.EsClient;
import com.navercorp.pinpoint.collector.dao.hbase.statistics.CalleeColumnName;
import com.navercorp.pinpoint.collector.dao.hbase.statistics.ColumnName;
import com.navercorp.pinpoint.collector.util.BeanToJson;
import com.navercorp.pinpoint.collector.util.EsIndexs;
import com.navercorp.pinpoint.common.server.util.AcceptedTimeService;
import com.navercorp.pinpoint.common.trace.ServiceType;
import com.navercorp.pinpoint.common.util.ApplicationMapStatisticsUtils;

@Repository("esMapStatisticsCallerDao")
public class ESMapStatisticsCallerDao implements MapStatisticsCallerDao {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Autowired
	private AcceptedTimeService acceptedTimeService;

	@Override
	public void flushAll() {
		// TODO Auto-generated method stub

	}

	@Override
	public void update(String callerApplicationName, ServiceType callerServiceType, String callerAgentId,
			String calleeApplicationName, ServiceType calleeServiceType, String calleeHost, int elapsed,
			boolean isError) {
		// TODO Auto-generated method stub

		if (callerApplicationName == null) {
			throw new NullPointerException("callerApplicationName must not be null");
		}
		if (calleeApplicationName == null) {
			throw new NullPointerException("calleeApplicationName must not be null");
		}

		if (logger.isDebugEnabled()) {
			logger.debug("[Caller] {} ({}) {} -> {} ({})[{}]", callerApplicationName, callerServiceType, callerAgentId,
					calleeApplicationName, calleeServiceType, calleeHost);
		}
		try {
			// there may be no endpoint in case of httpclient
			calleeHost = StringUtils.defaultString(calleeHost);
			final long acceptedTime = acceptedTimeService.getAcceptedTime();

			final short calleeSlotNumber = ApplicationMapStatisticsUtils.getSlotNumber(calleeServiceType, elapsed,
					isError);
			final ColumnName calleeColumnName = new CalleeColumnName(callerAgentId, calleeServiceType.getCode(),
					calleeApplicationName, calleeHost, calleeSlotNumber);
			//test-futao_TOMCAT^USER~test-futao^TOMCAT
			String key = callerApplicationName + "^" +callerServiceType.getName()+"~"+calleeApplicationName+"^"+calleeServiceType.getName();
			
			//JSONObject jsonbject = BeanToJson.toEsTime(calleeColumnName);
			JSONObject jsonbject = new JSONObject();
			jsonbject.put("callerAgentId", callerAgentId);
			jsonbject.put("key", key);
			jsonbject.put("callerServiceType", callerServiceType.getCode());
			jsonbject.put("calleeServiceType", calleeServiceType.getCode());
			jsonbject.put("calleeApplicationName", calleeApplicationName);
			jsonbject.put("callerApplicationName", callerApplicationName);
			jsonbject.put("calleeHost", calleeHost);
			jsonbject.put("calleeSlotNumber", calleeSlotNumber);
			jsonbject.put("hashCode", calleeColumnName.hashCode());
			jsonbject.put("acceptedTime", acceptedTime);
			jsonbject.put("elapsed", elapsed);
			jsonbject = BeanToJson.addEsTime(jsonbject);
			
			EsClient.client().prepareIndex(EsIndexs.APPLICATION_MAP_STATISTICS_CALLER_VER2, EsIndexs.TYPE)
			.setSource(jsonbject.toJSONString(), XContentType.JSON).get();
			/*if (useBulk) {
				lists.offer(jsonbject);
			} else {
				EsClient.client().prepareIndex(EsIndexs.APPLICATION_MAP_STATISTICS_CALLER_VER2, EsIndexs.TYPE)
						.setSource(jsonbject.toJSONString(), XContentType.JSON).get();
			}*/
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("esMapStatisticsCallerDao update error. Cause:{}", e.getMessage(), e);
		}

	}

}
