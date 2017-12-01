package com.navercorp.pinpoint.web.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.util.StopWatch;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.navercorp.pinpoint.web.applicationmap.rawdata.LinkDataMap;
import com.navercorp.pinpoint.web.dao.AgentInfoDao;
import com.navercorp.pinpoint.web.dao.hbase.HbaseMapResponseTimeDao;
import com.navercorp.pinpoint.web.dao.hbase.HbaseMapStatisticsCalleeDao;
import com.navercorp.pinpoint.web.scatter.ScatterData;
import com.navercorp.pinpoint.web.service.ApplicationFactory;
import com.navercorp.pinpoint.web.service.ScatterChartService;
import com.navercorp.pinpoint.web.util.LimitUtils;
import com.navercorp.pinpoint.web.vo.AgentInfo;
import com.navercorp.pinpoint.web.vo.Application;
import com.navercorp.pinpoint.web.vo.Range;
import com.navercorp.pinpoint.web.vo.ResponseTime;


/**
 * 
 * ClassName: MapStatController <br/> 
 * Function: 统计. <br/> 
 * date: 2017年1月23日 下午3:59:55 <br/> 
 * 
 * @author jacob 
 * @version  
 * @since JDK 1.8
 */
@Controller
@RequestMapping("/stat")
public class MapStatController {
	
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    
    @Autowired
    private ApplicationFactory applicationFactory;

    @Autowired
    private HbaseMapResponseTimeDao hbaseMapResponseTimeDao;
    
    @Autowired
    private HbaseMapStatisticsCalleeDao mapStatisticsCalleeDao;
    
    @Autowired
    private AgentInfoDao agentInfoDao;
    
    @Autowired
    private ScatterChartService scatter;
    
    
    @RequestMapping(value = "/getResponseTime", method = RequestMethod.GET, params="serviceTypeName")
    @ResponseBody
    public List<ResponseTime> getResponseTime(
    								          @RequestParam("applicationName") String applicationName,
    								          @RequestParam("serviceTypeName") String serviceTypeName,
    								          @RequestParam("from") long from,
    								          @RequestParam("to") long to) {
        final Range range = new Range(from, to);
        Application application = applicationFactory.createApplicationByTypeName(applicationName, serviceTypeName);
        
        logger.debug("getResponseTime() application:{} range:{}", application, range);
        
        return hbaseMapResponseTimeDao.selectResponseTime(application, range);
    }
    
    @RequestMapping(value = "/getMapCallee", method = RequestMethod.GET)
    @ResponseBody
    public LinkDataMap getMapCallee(@RequestParam("application") String applicationName,
    								@RequestParam("from") long from,
    								@RequestParam("to") long to) {
        final Range range = new Range(from, to);
        
        AgentInfo agentInfo = agentInfoDao.getInitialAgentInfo(applicationName);
        Application application = applicationFactory.createApplication(applicationName, agentInfo.getServiceTypeCode());
        
        logger.debug("getMapCallee() application:{} range:{}", application, range);
        
        return mapStatisticsCalleeDao.selectCallee(application, range);
    }
    
    @RequestMapping(value = "/getScatterData", method = RequestMethod.GET)
    @ResponseBody
    public ScatterData getScatterData(
            @RequestParam("application") String applicationName,
            @RequestParam("from") long from,
            @RequestParam("to") long to,
            @RequestParam(value = "xGroupUnit", required = false, defaultValue = "1") int xGroupUnit,
            @RequestParam(value = "yGroupUnit", required = false, defaultValue = "1") int yGroupUnit,
            @RequestParam(value = "limit", required = false, defaultValue = "5000") int limit,
            @RequestParam(value = "backwardDirection", required = false, defaultValue = "true") boolean backwardDirection) {
        limit = LimitUtils.checkRange(limit);
        StopWatch watch = new StopWatch();
        watch.start("getScatterData");
        final Range range = Range.createUncheckedRange(from, to);
        logger.debug("getScatterData() fetch scatter data. RANGE={}, X-Group-Unit:{}, Y-Group-Unit:{}, LIMIT={}, BACKWARD_DIRECTION:{}"
        		, range, xGroupUnit, yGroupUnit, limit, backwardDirection);
        ScatterData scatterData = scatter.selectScatterData(applicationName, range, xGroupUnit, yGroupUnit, limit, backwardDirection);
        watch.stop();
        logger.info("Fetch scatterData time : {}ms", watch.getLastTaskTimeMillis());
        return scatterData;
    }
    
    
}