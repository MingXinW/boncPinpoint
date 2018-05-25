package com.navercorp.pinpoint.collector.dao.proxy;

import javax.annotation.Resource;

import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.TraceDao;
import com.navercorp.pinpoint.common.server.bo.SpanBo;
import com.navercorp.pinpoint.common.server.bo.SpanChunkBo;

@Repository("traceDaoV2Proxy")
public class TraceDaoV2Proxy implements TraceDao {

	@Resource
	private TraceDao esTraceDaoV2;
	
	@Resource
	private TraceDao hbaseTraceDaoV2;
	
	@Override
	public void insert(SpanBo span) {
		// TODO Auto-generated method stub
		hbaseTraceDaoV2.insert(span);
		esTraceDaoV2.insert(span);
	}

	@Override
	public void insertSpanChunk(SpanChunkBo spanChunk) {
		// TODO Auto-generated method stub
		hbaseTraceDaoV2.insertSpanChunk(spanChunk);
		esTraceDaoV2.insertSpanChunk(spanChunk);
	}
}
