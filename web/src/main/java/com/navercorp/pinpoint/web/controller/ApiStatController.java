package com.navercorp.pinpoint.web.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import com.navercorp.pinpoint.web.applicationmap.ApplicationMap;
import com.navercorp.pinpoint.web.scatter.DotGroups;
import com.navercorp.pinpoint.web.scatter.ScatterAgentMetaData;
import com.navercorp.pinpoint.web.scatter.ScatterData;
import com.navercorp.pinpoint.web.service.CommonService;
import com.navercorp.pinpoint.web.service.MapService;
import com.navercorp.pinpoint.web.service.ScatterChartService;
import com.navercorp.pinpoint.web.util.LimitUtils;
import com.navercorp.pinpoint.web.util.Limiter;
import com.navercorp.pinpoint.web.util.TimeUtils;
import com.navercorp.pinpoint.web.vo.Application;
import com.navercorp.pinpoint.web.vo.Range;
import com.navercorp.pinpoint.web.vo.SearchOption;
import com.navercorp.pinpoint.web.vo.TransactionMetadataQuery;
import com.navercorp.pinpoint.web.vo.callstacks.Record;
import com.navercorp.pinpoint.web.vo.scatter.Dot;
import com.navercorp.pinpoint.web.vo.scatter.DotAgentInfo;

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
	private Limiter dateLimit;

	@Autowired
	private ScatterChartService scatter;

	private static final int DEFAULT_MAX_SEARCH_DEPTH = 8;

	private static final int DEFAULT_PERIOD = 300000;

	private static final String FUZZY_QUERY_SUFFIX = "_*";

	@RequestMapping(value = "/getTopSlowApis", method = RequestMethod.GET)
	@ResponseBody
	public List<Map> getTopSlowApis(@RequestParam("application") String applicationName,
			@RequestParam(value = "from", required = false, defaultValue = "0") long from,
			@RequestParam(value = "to", required = false, defaultValue = "0") long to,
			@RequestParam(value = "limit", required = false, defaultValue = "10000") int limit) {
		if (from == 0 && to == 0) {
			to = TimeUtils.getDelayLastTime();
			from = to - DEFAULT_PERIOD;
		}
		if (StringUtils.isEmpty(applicationName))
			throw new NullPointerException("applicationName must not be empty");
		final Range range = Range.createUncheckedRange(from, to);
		this.dateLimit.limit(range);

		SearchOption searchOption = new SearchOption(DEFAULT_MAX_SEARCH_DEPTH, DEFAULT_MAX_SEARCH_DEPTH);

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

			logger.info("getServerMap() application:{} range:{} searchOption:{}", app, range, searchOption);

			ScatterData scatterData = getScatterData(app.getName(), range, 1, 1, 10000, true);

			TransactionMetadataQuery query = parseTransaction(scatterData);

			if (query.size() > 0) {

				List<SpanBo> metadata = scatter.selectTransactionMetadata(query);
				logger.debug("application:{} api span size:{}", app.getName(), metadata.size());
				List<SpanBo> slowSpans = selectSlowSpans(metadata);

				allSlowSpans.addAll(slowSpans);
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
	private List<Map> chooseLimitedSlowApis(List<SpanBo> apis, int limit) {

		List<Map> topApis = new ArrayList<Map>();
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
			api.put("exception", span.getExceptionMessage());

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
	private List<SpanBo> selectSlowSpans(List<SpanBo> metadata) {

		Map<String, SpanBo> slowSpan = new HashMap<String, SpanBo>();
		for (SpanBo span : metadata) {
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
	 * @param range
	 * @param xGroupUnit
	 * @param yGroupUnit
	 * @param limit
	 * @param backwardDirection
	 * @return
	 */
	private ScatterData getScatterData(String applicationName, Range range, int xGroupUnit, int yGroupUnit, int limit,
			boolean backwardDirection) {
		limit = LimitUtils.checkRange(limit);
		StopWatch watch = new StopWatch();
		watch.start("getScatterData");
		logger.debug(
				"getScatterData() fetch scatter data. RANGE={}, X-Group-Unit:{}, Y-Group-Unit:{}, LIMIT={}, BACKWARD_DIRECTION:{}",
				range, xGroupUnit, yGroupUnit, limit, backwardDirection);
		ScatterData scatterData = scatter.selectScatterData(applicationName, range, xGroupUnit, yGroupUnit, limit,
				backwardDirection);
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
	private TransactionMetadataQuery parseTransaction(ScatterData scatterData) {

		final TransactionMetadataQuery query = new TransactionMetadataQuery();

		ScatterAgentMetaData metadata = scatterData.getScatterAgentMetadata();
		Map<Long, DotGroups> sortedScatterDataMap = scatterData.getSortedScatterDataMap();

		for (Map.Entry<Long, DotGroups> entry : sortedScatterDataMap.entrySet()) {

			DotGroups dotGroups = entry.getValue();
			Set<Dot> dotSet = dotGroups.getSortedDotSet();

			for (Dot dot : dotSet) {

				int agentId = metadata.getId(dot);

				String transactionId = "";
				if (agentId == -1) {
					transactionId = dot.getTransactionIdAsString();
				} else {
					transactionId = dot.getTransactionId().getAgentId() + "^"
							+ dot.getTransactionId().getAgentStartTime() + "^"
							+ dot.getTransactionId().getTransactionSequence();
				}

				final long time = dot.getAcceptedTime() + scatterData.getFrom();
				final int responseTime = dot.getElapsedTime();

				logger.debug("TransactionMetadataQuery:{}", transactionId + "," + time + "," + responseTime);

				query.addQueryCondition(transactionId, time, responseTime);
			}
		}
		logger.debug("TransactionMetadataQuery:{}", query);
		return query;
	}

}
