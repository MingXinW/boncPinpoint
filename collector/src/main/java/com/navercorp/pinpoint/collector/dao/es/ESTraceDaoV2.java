package com.navercorp.pinpoint.collector.dao.es;


import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.navercorp.pinpoint.collector.dao.TraceDao;
import com.navercorp.pinpoint.collector.dao.es.base.EsClient;
import com.navercorp.pinpoint.collector.util.EsIndexs;
import com.navercorp.pinpoint.common.server.bo.SpanBo;
import com.navercorp.pinpoint.common.server.bo.SpanChunkBo;
import com.navercorp.pinpoint.common.util.TransactionId;

@Repository("esTraceDaoV2")
public class ESTraceDaoV2 implements TraceDao {

	@Override
	public void insert(final SpanBo spanBo) {
		// TODO Auto-generated method stub
		if (spanBo == null) {
            throw new NullPointerException("spanBo must not be null");
        }


        TransactionId transactionId = spanBo.getTransactionId();
        String agentId = transactionId.getAgentId();
        String id = agentId + EsIndexs.ID_SEP + transactionId.getAgentStartTime() + EsIndexs.ID_SEP + transactionId.getTransactionSequence();
        try {
			ObjectMapper mapper = new ObjectMapper();
			byte[] json = mapper.writeValueAsBytes(spanBo);
			EsClient.client().prepareIndex(EsIndexs.TRACE_V2, EsIndexs.TYPE, id)
					.setSource(json).get();
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void insertSpanChunk(SpanChunkBo spanChunkBo) {
		// TODO Auto-generated method stub
		TransactionId transactionId = spanChunkBo.getTransactionId();
        
		 String id = transactionId.getAgentId();
	        
	        try {
				ObjectMapper mapper = new ObjectMapper();
				byte[] json = mapper.writeValueAsBytes(spanChunkBo);
				EsClient.client().prepareIndex(EsIndexs.TRACE_V2, EsIndexs.TYPE, id)
						.setSource(json).get();
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}

}
