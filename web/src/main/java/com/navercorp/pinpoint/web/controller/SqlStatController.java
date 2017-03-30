package com.navercorp.pinpoint.web.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.util.StopWatch;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.navercorp.pinpoint.web.service.CommonService;
import com.navercorp.pinpoint.web.service.ScatterChartService;
import com.navercorp.pinpoint.web.service.SpanService;
import com.navercorp.pinpoint.web.service.SqlSpanResult;
import com.navercorp.pinpoint.web.util.LimitUtils;
import com.navercorp.pinpoint.web.util.TimeUtils;
import com.navercorp.pinpoint.web.vo.Application;
import com.navercorp.pinpoint.web.vo.SelectedScatterArea;
import com.navercorp.pinpoint.web.vo.TransactionId;
import com.navercorp.pinpoint.web.vo.scatter.Dot;

/**
 * 
 * ClassName: SqlStatController <br/>
 * Function: SQL统计. <br/>
 * date: 2017年1月23日 下午3:59:55 <br/>
 * 
 * @author jacob
 * @version
 * @since JDK 1.8
 */
@Controller
@RequestMapping("/transaction")
public class SqlStatController {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Autowired
	private SpanService spanService;

	@Autowired
	private CommonService commonService;

	@Autowired
	private ScatterChartService scatter;


	private static final int DEFAULT_PERIOD = 300000;

	private static final String FUZZY_QUERY_SUFFIX = "_*";
	
	private static final int MAX_EXEC_TIME = 100000;
	
	

	@RequestMapping(value = "/getTopSlowSqls", method = RequestMethod.GET)
	@ResponseBody
	public List<Map<String,Object>> getTopSlowSql(@RequestParam("application") String applicationName,
			@RequestParam(value = "from", required = false, defaultValue = "0") long from,
			@RequestParam(value = "to", required = false, defaultValue = "0") long to,
			@RequestParam(value = "execTime", required = false, defaultValue = "5000") int execTime,
			@RequestParam(value = "limit", required = false, defaultValue = "20") int limit) {
		if (from == 0 && to == 0) {
			to = TimeUtils.getDelayLastTime();
			from = to - DEFAULT_PERIOD;
		}
		if (StringUtils.isEmpty(applicationName))
			throw new NullPointerException("applicationName must not be empty");
		
		SelectedScatterArea area = new SelectedScatterArea(from,to,execTime,MAX_EXEC_TIME,true);

		List<SqlSpanResult> sqlSpanList = new ArrayList<SqlSpanResult>();

		List<Application> apps = null;
		List<Application> allApps = commonService.selectAllApplicationNames();
		
		if (applicationName.endsWith(FUZZY_QUERY_SUFFIX)) {
			apps = filterApps(allApps, applicationName.substring(0, applicationName.length()-1));
		} else {
			apps = findAppsByName(allApps, applicationName);
		}

		if (apps == null || apps.size() == 0) {
			logger.info("can not find apps by appname:{}", applicationName);
			return  new ArrayList<Map<String,Object>>();
		}
		logger.info("application count is {}.", apps.size());

		for (Application app : apps) {

			logger.info("getServerMap() application:{} area:{} searchOption:{}", apps, area);

			//ScatterData scatterData = getScatterData(app.getName(), range, 1, 1, limit*10, true);
			List<Dot> scatterData = selectScatterData(app.getName(),area,limit * 10);
		//	List<QueryCondition> query = parseTransaction(scatterData);
			if(scatterData.size() > 0){
				for(Dot dot : scatterData){
					
					final TransactionId transactionId = dot.getTransactionId();
					final long focusTimestamp = dot.getAcceptedTime();
					// select spans
					List<SqlSpanResult> result = this.spanService.selectSqlSpan(transactionId, focusTimestamp);
					sqlSpanList.addAll(result);
				}
			}
		}
		return chooseLimitedSlowSqls(sqlSpanList, limit);
	}

	/**
	 * 根据applicationName 查找Application
	 * 
	 * @param allApps
	 * @param applicationName
	 * @return
	 */
	private List<Application> findAppsByName(List<Application> allApps, String applicationName) {

		List<Application> apps = new ArrayList<Application>();
		for (Application app : allApps) {
			if (applicationName.equals(app.getName())) {
				apps.add(app);
				break;
			}
		}
		return apps;
	}

	/**
	 * 通过前缀 查找Application
	 * 
	 * @param allApps
	 * @param suffix
	 * @return
	 */
	private List<Application> filterApps(List<Application> allApps, String suffix) {

		List<Application> apps = new ArrayList<Application>();
		for (Application app : allApps) {
			if (app.getName().startsWith(suffix)) {
				apps.add(app);
			}
		}
		return apps;
	}

	/**
	 * 获取 top limit 慢sql列表
	 * 
	 * @param apis
	 * @param limit
	 * @return
	 */
	private List<Map<String,Object>> chooseLimitedSlowSqls(List<SqlSpanResult> sqls, int limit) {

		List<Map<String,Object>> topApis = new ArrayList<Map<String,Object>>();
		List<SqlSpanResult> limitSqls = new ArrayList<SqlSpanResult>();

		sorteSqlSpanResult(sqls);

		if (sqls.size() < limit) {

			limitSqls = sqls;
		} else {

			limitSqls = sqls.subList(0, limit);
		}
		logger.info("SlowSql size:{} limitSqls size:{}", sqls.size(), limitSqls.size());
		for (SqlSpanResult result : limitSqls) {
			Map<String, Object> sql = new HashMap<String, Object>();
			sql.put("applicationName", result.getSpan().getApplicationId());
			sql.put("sql", result.getSql());
			sql.put("cost", result.getSpan().getElapsed());
			sql.put("traceId", result.getSpan().getSpanBo().getTransactionId());
			sql.put("path", result.getSpan().getSpanBo().getRpc());
			sql.put("collectorAcceptTime", result.getSpan().getSpanBo().getCollectorAcceptTime());
			sql.put("startTime", result.getSpan().getSpanBo().getStartTime());
			sql.put("agentId", result.getSpan().getSpanBo().getAgentId());
			sql.put("remoteAddr", result.getSpan().getSpanBo().getRemoteAddr());
			sql.put("endPoint", result.getSpan().getSpanBo().getEndPoint());
			sql.put("exception", result.getSpan().getExceptionMessage());

			topApis.add(sql);
		}
		return topApis;
	}

	/**
	 * 对慢sql span排序
	 * 
	 * @param SpanAlignList
	 */
	private void sorteSqlSpanResult(List<SqlSpanResult> SpanAlignList) {

		Collections.sort(SpanAlignList, new Comparator<SqlSpanResult>() {
			public int compare(SqlSpanResult o1, SqlSpanResult o2) {

				final long elapsed1 = o1.getSpan().getElapsed();
				final long elapsed2 = o2.getSpan().getElapsed();
				if (elapsed1 > elapsed2) {
					return -1;
				} else {
					if (elapsed1 == elapsed2) {
						return 0;
					} else {
						return 1;
					}
				}
			}

		});
	}
	
	/**
	 * 
	 * @param applicationName
	 * @param area
	 * @param limit
	 * @return
	 */
	private List<Dot> selectScatterData(String applicationName, SelectedScatterArea area,int limit){
		limit = LimitUtils.checkRange(limit);
		StopWatch watch = new StopWatch();
		watch.start("getScatterData");
		logger.debug(
				"selectScatterData() fetch scatter data. applicationName={}, LIMIT={}, AREA:{}",applicationName, limit, area);
		 List<Dot> scatterData = scatter.selectScatterData(applicationName, area, null, 0, limit);
		watch.stop();
		logger.info("Fetch scatterData time : {}ms", watch.getLastTaskTimeMillis());
		return scatterData;
	}
	
}
