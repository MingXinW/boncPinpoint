package com.navercorp.pinpoint.collector.dao.es;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.navercorp.pinpoint.collector.dao.TraceDao;
import com.navercorp.pinpoint.collector.dao.es.base.EsClient;
import com.navercorp.pinpoint.collector.util.BeanToJson;
import com.navercorp.pinpoint.collector.util.EsIndexs;
import com.navercorp.pinpoint.common.server.bo.AnnotationBo;
import com.navercorp.pinpoint.common.server.bo.SpanBo;
import com.navercorp.pinpoint.common.server.bo.SpanChunkBo;
import com.navercorp.pinpoint.common.server.bo.SpanEventBo;
import com.navercorp.pinpoint.common.util.AnnotationTranscoder;

@Repository("esTraceDaoV2")
public class ESTraceDaoV2 implements TraceDao {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	private static final AnnotationTranscoder transcoder = new AnnotationTranscoder();
	
	@Override
	public void insert(final SpanBo spanBo) {
		// TODO Auto-generated method stub
		if (spanBo == null) {
			throw new NullPointerException("spanBo must not be null");
		}

		/*TransactionId transactionId = spanBo.getTransactionId();
		String agentId = transactionId.getAgentId();
		String id = agentId + EsIndexs.ID_SEP + transactionId.getAgentStartTime() + EsIndexs.ID_SEP
				+ transactionId.getTransactionSequence();*/
		try {
			JSONObject jsonObject = BeanToJson.toEsTime(spanBo);
			addAnnotationValueType(spanBo, jsonObject);
			EsClient.client().prepareIndex(EsIndexs.TRACE_V2, EsIndexs.TYPE)
			.setSource(jsonObject.toJSONString(),XContentType.JSON).get();
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			logger.error("esTraceDaoV2 insert error. Cause:{}", e.getMessage(), e);
		}
	}

	@Override
	public void insertSpanChunk(SpanChunkBo spanChunkBo) {
		// TODO Auto-generated method stub
		/*TransactionId transactionId = spanChunkBo.getTransactionId();

		String agentId = transactionId.getAgentId();
		String id = agentId + EsIndexs.ID_SEP + transactionId.getAgentStartTime() + EsIndexs.ID_SEP
				+ transactionId.getTransactionSequence();*/
		try {
			JSONObject jsonObject = BeanToJson.toEsTime(spanChunkBo);
			addAnnotationValueType(spanChunkBo, jsonObject);
			EsClient.client().prepareIndex(EsIndexs.getIndex(EsIndexs.TRACE_CHUNK_V2), EsIndexs.TYPE)
			.setSource(jsonObject.toJSONString(),XContentType.JSON).get();
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			logger.error("esTraceDaoV2 insertSpanChunk error. Cause:{}", e.getMessage(), e);
		}
	}
	
	private void addAnnotationValueType(SpanChunkBo spanChunkBo, JSONObject jsonObject) {
		List<SpanEventBo> spanEventBoList = spanChunkBo.getSpanEventBoList();
		JSONArray spanEventBoArr = jsonObject.getJSONArray("spanEventBoList");
		
		for(int i = 0; i < spanEventBoList.size(); i++) {
			SpanEventBo spanEventBo = spanEventBoList.get(i);
			JSONObject spanEventObj = spanEventBoArr.getJSONObject(i);
			
			List<AnnotationBo> annotationBoList = spanEventBo.getAnnotationBoList();
			JSONArray annotationBoArr = spanEventObj.getJSONArray("annotationBoList");
			
			if (CollectionUtils.isNotEmpty(annotationBoList)) {
				for(int j = 0; j < annotationBoList.size(); j++) {
					AnnotationBo annotationBo = annotationBoList.get(j);
					JSONObject annotationBoObj = annotationBoArr.getJSONObject(j);
					
					Object value = annotationBo.getValue();
					JSONObject valueObj = new JSONObject();
					byte typeCode = transcoder.getTypeCode(value);
					valueObj.put("typeCode",  typeCode);
					valueObj.put("encoded", (transcoder.encode(value, typeCode)));
					annotationBoObj.put("value", valueObj);
				} 
			}
		}
		
	}
	
	private void addAnnotationValueType(SpanBo spanBo, JSONObject jsonObject) {
		List<SpanEventBo> spanEventBoList = spanBo.getSpanEventBoList();
		JSONArray spanEventBoArr = jsonObject.getJSONArray("spanEventBoList");
		
		for(int i = 0; i < spanEventBoList.size(); i++) {
			SpanEventBo spanEventBo = spanEventBoList.get(i);
			JSONObject spanEventObj = spanEventBoArr.getJSONObject(i);
			
			List<AnnotationBo> annotationBoList = spanEventBo.getAnnotationBoList();
			JSONArray annotationBoArr = spanEventObj.getJSONArray("annotationBoList");
			
			if (CollectionUtils.isNotEmpty(annotationBoList)) {
				for(int j = 0; j < annotationBoList.size(); j++) {
					AnnotationBo annotationBo = annotationBoList.get(j);
					JSONObject annotationBoObj = annotationBoArr.getJSONObject(j);
					
					Object value = annotationBo.getValue();
					JSONObject valueObj = new JSONObject();
					byte typeCode = transcoder.getTypeCode(value);
					valueObj.put("typeCode",  typeCode);
					valueObj.put("encoded", (transcoder.encode(value, typeCode)));
					annotationBoObj.put("value", valueObj);
				} 
			}
		}
	}

}
