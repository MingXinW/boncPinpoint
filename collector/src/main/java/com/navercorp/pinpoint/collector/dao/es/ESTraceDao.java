package com.navercorp.pinpoint.collector.dao.es;




import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.navercorp.pinpoint.collector.dao.TraceDao;
import com.navercorp.pinpoint.collector.dao.es.base.EsClient;
import com.navercorp.pinpoint.collector.util.EsIndexs;
import com.navercorp.pinpoint.common.server.bo.AnnotationBo;
import com.navercorp.pinpoint.common.server.bo.SpanBo;
import com.navercorp.pinpoint.common.server.bo.SpanChunkBo;
import com.navercorp.pinpoint.common.server.bo.SpanEventBo;
import com.navercorp.pinpoint.common.util.TransactionId;

@Repository("esTraceDao")
public class ESTraceDao implements TraceDao {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@Override
	public void insert(final SpanBo spanBo) {
		// TODO Auto-generated method stub
		if (spanBo == null) {
            throw new NullPointerException("span must not be null");
        }

        TransactionId transactionId = spanBo.getTransactionId();
        String agentId = transactionId.getAgentId();
        String id = agentId + EsIndexs.ID_SEP + transactionId.getAgentStartTime() + EsIndexs.ID_SEP + transactionId.getTransactionSequence();
        parseSpanBo(spanBo);
        try {
			ObjectMapper mapper = new ObjectMapper();
			byte[] json = mapper.writeValueAsBytes(spanBo);
			EsClient.client().prepareIndex(EsIndexs.TRACES, EsIndexs.TYPE, id)
					.setSource(json).get();
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			logger.error("esTraceDao insert error. Cause:{}", e.getMessage(), e);
		}
	}

	@Override
	public void insertSpanChunk(SpanChunkBo spanChunkBo) {
		// TODO Auto-generated method stub
		TransactionId transactionId = spanChunkBo.getTransactionId();
		String agentId = transactionId.getAgentId();
        String id = agentId + EsIndexs.ID_SEP + transactionId.getAgentStartTime() + EsIndexs.ID_SEP + transactionId.getTransactionSequence();
        parseSpanChunkBo(spanChunkBo);
        try {
			ObjectMapper mapper = new ObjectMapper();
			byte[] json = mapper.writeValueAsBytes(spanChunkBo);
			EsClient.client().prepareIndex(EsIndexs.TRACES, EsIndexs.TYPE, id)
					.setSource(json).get();
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			logger.error("esTraceDao insertSpanChunk error. Cause:{}", e.getMessage(), e);
		}
	}
	
	private void parseSpanChunkBo(SpanChunkBo spanChunkBo) {
		List<SpanEventBo> spanEventBoList = spanChunkBo.getSpanEventBoList();
		Iterator<SpanEventBo> its = spanEventBoList.iterator();
		while(its.hasNext()) {
			SpanEventBo spanEventBo = its.next();
			List<AnnotationBo> spanEventBos = spanEventBo.getAnnotationBoList();
			if (CollectionUtils.isNotEmpty(spanEventBos)) {
				Iterator<AnnotationBo> itsSpan = spanEventBos.iterator();
				while(itsSpan.hasNext()) {
					AnnotationBo annotationBo = itsSpan.next();
					Object value = annotationBo.getValue();
					String strValue = String.valueOf(value);
					annotationBo.setValue(strValue);
				}
			}
		}
	}
	
	private void parseSpanBo(SpanBo spanBo) {
		List<SpanEventBo> spanEventBoList = spanBo.getSpanEventBoList();
		Iterator<SpanEventBo> its = spanEventBoList.iterator();
		while(its.hasNext()) {
			SpanEventBo spanEventBo = its.next();
			List<AnnotationBo> spanEventBos = spanEventBo.getAnnotationBoList();
			if (CollectionUtils.isNotEmpty(spanEventBos)) {
				Iterator<AnnotationBo> itsSpan = spanEventBos.iterator();
				while(itsSpan.hasNext()) {
					AnnotationBo annotationBo = itsSpan.next();
					Object value = annotationBo.getValue();
					String strValue = String.valueOf(value);
					annotationBo.setValue(strValue);
				}
			}
		}
	}

}
