package com.navercorp.pinpoint.collector.dao.es;

import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.alibaba.fastjson.JSONObject;
import com.navercorp.pinpoint.collector.dao.MapResponseTimeDao;
import com.navercorp.pinpoint.collector.dao.es.base.EsClient;
import com.navercorp.pinpoint.collector.dao.hbase.statistics.ColumnName;
import com.navercorp.pinpoint.collector.dao.hbase.statistics.ResponseColumnName;
import com.navercorp.pinpoint.collector.util.BeanToJson;
import com.navercorp.pinpoint.collector.util.EsIndexs;
import com.navercorp.pinpoint.common.server.util.AcceptedTimeService;
import com.navercorp.pinpoint.common.trace.ServiceType;
import com.navercorp.pinpoint.common.util.ApplicationMapStatisticsUtils;

@Repository("esMapResponseTimeDao")
public class ESMapResponseTimeDao implements MapResponseTimeDao{

	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@Autowired
    private AcceptedTimeService acceptedTimeService;
	
	@Override
	public void flushAll() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void received(String applicationName, ServiceType applicationServiceType, String agentId, int elapsed,
			boolean isError) {
		// TODO Auto-generated method stub
		if (applicationName == null) {
            throw new NullPointerException("applicationName must not be null");
        }
        if (agentId == null) {
            throw new NullPointerException("agentId must not be null");
        }

        if (logger.isDebugEnabled()) {
            logger.debug("[Received] {} ({})[{}]", applicationName, applicationServiceType, agentId);
        }

        try {
	        // make row key. rowkey is me
	        final long acceptedTime = acceptedTimeService.getAcceptedTime();
	        
	        final short slotNumber = ApplicationMapStatisticsUtils.getSlotNumber(applicationServiceType, elapsed, isError);
	        final ColumnName selfColumnName = new ResponseColumnName(agentId, slotNumber);
	        
	        JSONObject jsonbject = new JSONObject();
			jsonbject.put("applicationName", applicationName);
			jsonbject.put("applicationServiceType", applicationServiceType.getCode());
			jsonbject.put("agentId", agentId);
			jsonbject.put("hashCode", selfColumnName.hashCode());
			jsonbject.put("acceptedTime", acceptedTime);
			jsonbject.put("elapsed", elapsed);
			jsonbject = BeanToJson.addEsTime(jsonbject);
			
			EsClient.client().prepareIndex(EsIndexs.APPLICATION_MAP_STATISTICS_SELF_VER2, EsIndexs.TYPE)
			.setSource(jsonbject.toJSONString(), XContentType.JSON).get();
        } catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("esMapStatisticsCalleeDao update error. Cause:{}", e.getMessage(), e);
		}
	}
}
