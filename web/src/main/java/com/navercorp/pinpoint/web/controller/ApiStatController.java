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

import com.navercorp.pinpoint.common.bo.SpanBo;
import com.navercorp.pinpoint.web.scatter.ScatterData;
import com.navercorp.pinpoint.web.service.CommonService;
import com.navercorp.pinpoint.web.service.ScatterChartService;
import com.navercorp.pinpoint.web.util.LimitUtils;
import com.navercorp.pinpoint.web.util.TimeUtils;
import com.navercorp.pinpoint.web.vo.Application;
import com.navercorp.pinpoint.web.vo.Range;
import com.navercorp.pinpoint.web.vo.SearchOption;
import com.navercorp.pinpoint.web.vo.SelectedScatterArea;
import com.navercorp.pinpoint.web.vo.TransactionMetadataQuery;
import com.navercorp.pinpoint.web.vo.scatter.Dot;

/**
 * 
 * ClassName: ApiStatController <br/>
 * Function: API统计. <br/>
 * date: 2017年1月23日 下午3:59:55 <br/>
 * 
 * @author jacob
 * @version
 * @since JDK 1.8
 */
@Controller
@RequestMapping("/transaction")
public class ApiStatController {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Autowired
	private CommonService commonService;

	@Autowired
	private ScatterChartService scatter;
	
	private static final int DEFAULT_PERIOD = 300000;

	private static final String FUZZY_QUERY_SUFFIX = "_*";
	
	private static final int MAX_EXEC_TIME = 100000;

	@RequestMapping(value = "/getTopSlowApis", method = RequestMethod.GET)
	@ResponseBody
	public List<Map<String,Object>> getTopSlowApis(@RequestParam("application") String applicationName,
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

		List<Application> apps = null;
		List<Application> allApps = commonService.selectAllApplicationNames();
		if (applicationName.endsWith(FUZZY_QUERY_SUFFIX)) {
			apps = filterApps(allApps, applicationName.substring(0, applicationName.length()-1));
		} else {
			apps = findAppsByName(allApps, applicationName);
		}

		if (apps == null || apps.size() == 0) {
			logger.info("can not find apps by appname:{}", applicationName);
			return null;
		}
		logger.info("application count is {}.", apps.size());

		List<SpanBo> allSlowSpans = new ArrayList<SpanBo>();

		for (Application app : apps) {

			logger.info("getServerMap() application:{} area:{} searchOption:{}", apps, area);

			/*ScatterData scatterData = getScatterData(app.getName(), range, 1, 1, 10000, true);
			TransactionMetadataQuery query = parseTransaction(scatterData);*/
			List<Dot> scatterData = selectScatterData(app.getName(),area,limit * 10);
			TransactionMetadataQuery query = parseTransaction(scatterData);

			if (query.size() > 0) {

				List<SpanBo> metadata = scatter.selectTransactionMetadata(query);
				logger.debug("application:{} api span size:{}", app.getName(), metadata.size());
				
				List<SpanBo> slowSpans = selectSlowSpans(metadata, execTime);
				allSlowSpans.addAll(slowSpans);
			}
		}

		return chooseLimitedSlowApis(allSlowSpans, limit);
	}

	@RequestMapping(value = "/getUnsuccessTraces", method = RequestMethod.GET)
	@ResponseBody
	public List<Map<String,Object>> getUnsuccessTraces(@RequestParam("application") String applicationName,
			@RequestParam(value = "from", required = false, defaultValue = "0") long from,
			@RequestParam(value = "to", required = false, defaultValue = "0") long to,
			@RequestParam(value = "limit", required = false, defaultValue = "10000") int limit) {
		if (from == 0 && to == 0) {
			to = TimeUtils.getDelayLastTime();
			from = to - DEFAULT_PERIOD;
		}
		if (StringUtils.isEmpty(applicationName))
			throw new NullPointerException("applicationName must not be empty");

		SelectedScatterArea area = new SelectedScatterArea(from,to,0,MAX_EXEC_TIME,true);

		List<Application> apps = null;
		List<Application> allApps = commonService.selectAllApplicationNames();
		if (applicationName.endsWith(FUZZY_QUERY_SUFFIX)) {
			apps = filterApps(allApps, applicationName.substring(0, applicationName.length()-1));
		} else {
			apps = findAppsByName(allApps, applicationName);
		}

		if (apps == null || apps.size() == 0) {
			logger.info("can not find apps by appname:{}", applicationName);
			return null;
		}
		logger.info("application count is {}.", apps.size());

		List<SpanBo> allSlowSpans = new ArrayList<SpanBo>();

		for (Application app : apps) {

			logger.info("getServerMap() application:{} area:{} searchOption:{}", apps, area);

			/*ScatterData scatterData = getScatterData(app.getName(), range, 1, 1, 10000, true);
			TransactionMetadataQuery query = parseTransaction(scatterData);*/
			List<Dot> scatterData = selectScatterData(app.getName(),area,limit * 10);
			
			TransactionMetadataQuery query = parseUnsuccessTransaction(scatterData);

			if (query.size() > 0) {

				List<SpanBo> metadata = scatter.selectTransactionMetadata(query);
				logger.debug("application:{} api span size:{}", app.getName(), metadata.size());
				
				List<SpanBo> unsuccessSpans = selectSlowSpans(metadata, 0);
				allSlowSpans.addAll(unsuccessSpans);
			}
		}

		return chooseLimitedSlowApis(allSlowSpans, limit);
	}

	@RequestMapping(value = "/getTraces", method = RequestMethod.GET)
	@ResponseBody
	public List<Map<String,Object>> getTraces(@RequestParam("application") String applicationName,
			@RequestParam(value = "from", required = false, defaultValue = "0") long from,
			@RequestParam(value = "to", required = false, defaultValue = "0") long to,
			@RequestParam(value = "limit", required = false, defaultValue = "3000") int limit) {
		if (from == 0 && to == 0) {
			to = TimeUtils.getDelayLastTime();
			from = to - DEFAULT_PERIOD;
		}
		if (StringUtils.isEmpty(applicationName))
			throw new NullPointerException("applicationName must not be empty");

		SelectedScatterArea area = new SelectedScatterArea(from,to,0,MAX_EXEC_TIME,true);

		List<Application> apps = null;
		List<Application> allApps = commonService.selectAllApplicationNames();
		if (applicationName.endsWith(FUZZY_QUERY_SUFFIX)) {
			apps = filterApps(allApps, applicationName.substring(0, applicationName.length()-1));
		} else {
			apps = findAppsByName(allApps, applicationName);
		}

		if (apps == null || apps.size() == 0) {
			logger.info("can not find apps by appname:{}", applicationName);
			return null;
		}
		logger.info("application count is {}.", apps.size());

		List<SpanBo> allSlowSpans = new ArrayList<SpanBo>();

		for (Application app : apps) {

			logger.info("getServerMap() application:{} area:{} searchOption:{}", apps, area);

			/*ScatterData scatterData = getScatterData(app.getName(), range, 1, 1, 10000, true);
			TransactionMetadataQuery query = parseTransaction(scatterData);*/
			List<Dot> scatterData = selectScatterData(app.getName(),area,limit * 10);
			TransactionMetadataQuery query = parseTransaction(scatterData);

			if (query.size() > 0) {

				List<SpanBo> metadata = scatter.selectTransactionMetadata(query);
				logger.debug("application:{} api span size:{}", app.getName(), metadata.size());
				
				allSlowSpans.addAll(metadata);
			}
		}

		return chooseLimitedSlowApis(allSlowSpans, limit);
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
	 * 获取 top limit 慢api列表
	 * 
	 * @param allSlowApis
	 * @param limit
	 * @return
	 */
	private List<Map<String,Object>> chooseLimitedSlowApis(List<SpanBo> apis, int limit) {

		List<Map<String,Object>> topApis = new ArrayList<Map<String,Object>>();
		List<SpanBo> limitApis = new ArrayList<SpanBo>();

		sorteSpan(apis);

		if (apis.size() < limit) {

			limitApis = apis;
		} else {

			limitApis = apis.subList(0, limit);
		}
		logger.info("SlowApis size:{} limitApis size:{}", apis.size(), limitApis.size());
		for (SpanBo span : limitApis) {
			Map<String, Object> api = new HashMap<String, Object>();
			api.put("applicationName", span.getApplicationId());
			api.put("path", span.getRpc());
			api.put("cost", span.getElapsed());
			api.put("traceId", span.getTransactionId());
			api.put("collectorAcceptTime", span.getCollectorAcceptTime());
			api.put("startTime", span.getStartTime());
			api.put("agentId", span.getAgentId());
			api.put("remoteAddr", span.getRemoteAddr());
			api.put("endPoint", span.getEndPoint());
			api.put("errCode", span.getErrCode());
			api.put("exception", span.getExceptionMessage());
			api.put("exceptionClass", span.getExceptionClass());
			api.put("errCode", span.getErrCode());
			api.put("serviceType", span.getServiceType());
			api.put("applicationServiceType", span.getApplicationServiceType());
			api.put("apiId", span.getApiId());

			topApis.add(api);
		}
		return topApis;
	}

	/**
	 * 对慢api span排序 desc
	 * 
	 * @param allSlowApis
	 */
	private void sorteSpan(List<SpanBo> allSlowApis) {

		Collections.sort(allSlowApis, new Comparator<SpanBo>() {
			public int compare(SpanBo o1, SpanBo o2) {

				final int elapsed1 = o1.getElapsed();
				final int elapsed2 = o2.getElapsed();
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
	 * 根据 api 从多次访问中获取最慢的span
	 * 
	 * @param metadata
	 * @return
	 */
	private List<SpanBo> selectSlowSpans(List<SpanBo> metadata, int execTime) {

		Map<String, SpanBo> slowSpan = new HashMap<String, SpanBo>();
		for (SpanBo span : metadata) {
			if(span.getElapsed() < execTime){
				continue;
			}
			
			String path = span.getRpc();
			int res = span.getElapsed();
			if (slowSpan.containsKey(path)) {
				SpanBo oldSpan = slowSpan.get(path);
				int oldRes = oldSpan.getElapsed();
				if (oldRes < res)
					slowSpan.put(path, span);
			} else {
				slowSpan.put(path, span);
			}
		}
		logger.debug("selectSlowSpans() api path:{}", slowSpan.keySet());
		return new ArrayList<SpanBo>(slowSpan.values());
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
	/**
	 * 组织span查询条件
	 * 
	 * @param scatterData
	 * @return
	 */
	private TransactionMetadataQuery parseTransaction(List<Dot> scatterData) {

		final TransactionMetadataQuery query = new TransactionMetadataQuery();

			for (Dot dot : scatterData) {
				
				String transactionId = dot.getTransactionIdAsString();
				final long time = dot.getAcceptedTime();
				final int responseTime = dot.getElapsedTime();

				logger.debug("TransactionMetadataQuery:{}", transactionId + "," + time + "," + responseTime);

				query.addQueryCondition(transactionId, time, responseTime);
			}
	
		return query;
	}
	/**
	 * 组织span查询条件
	 * 
	 * @param scatterData
	 * @return
	 */
	private TransactionMetadataQuery parseUnsuccessTransaction(List<Dot> scatterData) {

		final TransactionMetadataQuery query = new TransactionMetadataQuery();

			for (Dot dot : scatterData) {
				
				if(dot.getSimpleExceptionCode() == Dot.SUCCESS_STATE){
					continue;
				}
				
				String transactionId = dot.getTransactionIdAsString();
				final long time = dot.getAcceptedTime();
				final int responseTime = dot.getElapsedTime();

				logger.debug("TransactionMetadataQuery:{}", transactionId + "," + time + "," + responseTime);

				query.addQueryCondition(transactionId, time, responseTime);
			}
	
		return query;
	}

}
