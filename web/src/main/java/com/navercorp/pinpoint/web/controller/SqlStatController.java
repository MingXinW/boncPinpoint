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

import com.navercorp.pinpoint.common.bo.AnnotationBo;
import com.navercorp.pinpoint.common.bo.SpanBo;
import com.navercorp.pinpoint.common.trace.AnnotationKey;
import com.navercorp.pinpoint.web.applicationmap.MapWrap;
import com.navercorp.pinpoint.web.calltree.span.CallTreeIterator;
import com.navercorp.pinpoint.web.calltree.span.CallTreeNode;
import com.navercorp.pinpoint.web.calltree.span.SpanAlign;
import com.navercorp.pinpoint.web.scatter.DotGroups;
import com.navercorp.pinpoint.web.scatter.ScatterAgentMetaData;
import com.navercorp.pinpoint.web.scatter.ScatterData;
import com.navercorp.pinpoint.web.service.CommonService;
import com.navercorp.pinpoint.web.service.ScatterChartService;
import com.navercorp.pinpoint.web.service.SpanResult;
import com.navercorp.pinpoint.web.service.SpanService;
import com.navercorp.pinpoint.web.util.LimitUtils;
import com.navercorp.pinpoint.web.util.Limiter;
import com.navercorp.pinpoint.web.util.TimeUtils;
import com.navercorp.pinpoint.web.vo.Application;
import com.navercorp.pinpoint.web.vo.Range;
import com.navercorp.pinpoint.web.vo.SearchOption;
import com.navercorp.pinpoint.web.vo.TransactionId;
import com.navercorp.pinpoint.web.vo.TransactionMetadataQuery;
import com.navercorp.pinpoint.web.vo.TransactionMetadataQuery.QueryCondition;
import com.navercorp.pinpoint.web.vo.callstacks.Record;
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
	private Limiter dateLimit;

	@Autowired
	private ScatterChartService scatter;

	private static final int DEFAULT_MAX_SEARCH_DEPTH = 8;

	private static final int DEFAULT_PERIOD = 300000;

	private static final String FUZZY_QUERY_SUFFIX = "_*";

	@RequestMapping(value = "/getTopSlowSqls", method = RequestMethod.GET)
	@ResponseBody
	public List<Map> getTopSlowSql(@RequestParam("application") String applicationName,
			@RequestParam(value = "from", required = false, defaultValue = "0") long from,
			@RequestParam(value = "to", required = false, defaultValue = "0") long to,
			@RequestParam(value = "limit", required = false, defaultValue = "10000") int limit) {
		if (from == 0 && to == 0) {
			to = TimeUtils.getDelayLastTime();
			from = to - DEFAULT_PERIOD;
		}
		if (StringUtils.isEmpty(applicationName))
			throw new NullPointerException("applicationName must not be empty");

		final Range range = new Range(from, to);
		this.dateLimit.limit(range);

		List<SpanAlign> sqlSpanList = new ArrayList<SpanAlign>();

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

		for (Application app : apps) {

			logger.info("getServerMap() application:{} range:{} searchOption:{}", apps, range, searchOption);

			ScatterData scatterData = getScatterData(app.getName(), range, 1, 1, 10000, true);
			List<QueryCondition> query = parseTransaction(scatterData);

			if (query.size() > 0) {
				for (QueryCondition queryCondition : query) {

					final TransactionId transactionId = queryCondition.getTransactionId();
					final long focusTimestamp = queryCondition.getCollectorAcceptorTime();
					// select spans
					final SpanResult spanResult = this.spanService.selectSpan(transactionId, focusTimestamp);

					final CallTreeIterator callTreeIterator = spanResult.getCallTree();

					findSlowSqlSpan(callTreeIterator, sqlSpanList);

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
	private List<Map> chooseLimitedSlowSqls(List<SpanAlign> sqls, int limit) {

		List<Map> topApis = new ArrayList<Map>();
		List<SpanAlign> limitSqls = new ArrayList<SpanAlign>();

		sorteSpanAlign(sqls);

		if (sqls.size() < limit) {

			limitSqls = sqls;
		} else {

			limitSqls = sqls.subList(0, limit);
		}
		logger.info("SlowSql size:{} limitSqls size:{}", sqls.size(), limitSqls.size());
		for (SpanAlign span : limitSqls) {
			Map<String, Object> sql = new HashMap<String, Object>();
			sql.put("applicationName", span.getApplicationId());
			AnnotationBo sqlIdAnnotation = findAnnotation(span.getAnnotationBoList(), AnnotationKey.SQL.getCode());
			sql.put("sql", sqlIdAnnotation.getValue());
			sql.put("cost", span.getElapsed());
			sql.put("traceId", span.getSpanBo().getTransactionId());
			sql.put("path", span.getSpanBo().getRpc());
			sql.put("collectorAcceptTime", span.getSpanBo().getCollectorAcceptTime());
			sql.put("startTime", span.getSpanBo().getStartTime());
			sql.put("agentId", span.getSpanBo().getAgentId());
			sql.put("remoteAddr", span.getSpanBo().getRemoteAddr());
			sql.put("endPoint", span.getSpanBo().getEndPoint());
			sql.put("exception", span.getExceptionMessage());

			topApis.add(sql);
		}
		return topApis;
	}

	/**
	 * 找到慢sql的spanAlign
	 * 
	 * @param callTreeIterator
	 * @param sqlSpanMap
	 */
	private void findSlowSqlSpan(CallTreeIterator callTreeIterator, List<SpanAlign> sqlSpanList) {

		while (callTreeIterator.hasNext()) {
			final CallTreeNode node = callTreeIterator.next();
			if (node == null) {
				logger.warn("Corrupt CallTree found : {}", callTreeIterator.toString());
				throw new IllegalStateException("CallTree corrupted");
			}
			final SpanAlign align = node.getValue();
			if (!align.getAnnotationBoList().isEmpty()) {
				List<AnnotationBo> annotationBoList = align.getAnnotationBoList();
				AnnotationBo sqlIdAnnotation = findAnnotation(annotationBoList, AnnotationKey.SQL.getCode());

				if (sqlIdAnnotation != null) {
					sqlSpanList.add(align);
				}
			}
		}

	}

	/**
	 * 对慢sql span排序
	 * 
	 * @param SpanAlignList
	 */
	private void sorteSpanAlign(List<SpanAlign> SpanAlignList) {

		Collections.sort(SpanAlignList, new Comparator<SpanAlign>() {
			public int compare(SpanAlign o1, SpanAlign o2) {

				final long elapsed1 = o1.getElapsed();
				final long elapsed2 = o2.getElapsed();
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

	private AnnotationBo findAnnotation(List<AnnotationBo> annotationBoList, int key) {
		for (AnnotationBo annotationBo : annotationBoList) {
			if (key == annotationBo.getKey()) {
				return annotationBo;
			}
		}
		return null;
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
	 * 组织查询
	 * 
	 * @param scatterData
	 * @return
	 */
	private List<QueryCondition> parseTransaction(ScatterData scatterData) {

		final List<QueryCondition> query = new ArrayList<QueryCondition>();

		ScatterAgentMetaData metadata = scatterData.getScatterAgentMetadata();
		Map<Long, DotGroups> sortedScatterDataMap = scatterData.getSortedScatterDataMap();

		for (Map.Entry<Long, DotGroups> entry : sortedScatterDataMap.entrySet()) {

			DotGroups dotGroups = entry.getValue();
			Set<Dot> dotSet = dotGroups.getSortedDotSet();

			for (Dot dot : dotSet) {

				int agentId = metadata.getId(dot);
				String traceId = agentId == -1 ? dot.getTransactionIdAsString()
						: dot.getTransactionId().getTransactionSequence() + "";

				final String transactionId = dot.getTransactionId().getAgentId() + "^"
						+ dot.getTransactionId().getAgentStartTime() + "^" + traceId;
				final String time = dot.getAcceptedTime() + "";
				final String responseTime = dot.getElapsedTime() + "";

				logger.debug("TransactionMetadataQuery:{}", traceId + "," + time + "," + responseTime);

				query.add(new QueryCondition(new TransactionId(transactionId), Long.parseLong(time),
						Integer.parseInt(responseTime)));
			}
		}
		logger.debug("TransactionMetadataQuery:{}", query);
		return query;
	}
}
