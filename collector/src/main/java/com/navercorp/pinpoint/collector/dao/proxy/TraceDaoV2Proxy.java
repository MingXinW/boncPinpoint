package com.navercorp.pinpoint.collector.dao.proxy;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.TraceDao;
import com.navercorp.pinpoint.common.server.bo.SpanBo;
import com.navercorp.pinpoint.common.server.bo.SpanChunkBo;

@Repository("traceDaoV2Proxy")
public class TraceDaoV2Proxy implements TraceDao {

	@Autowired
	@Qualifier("esTraceDaoV2")
	private TraceDao esTraceDaoV2;
	
	@Autowired(required = false)
	@Qualifier("hbaseTraceDaoV2")
	private TraceDao hbaseTraceDaoV2;
	
	@Override
	public void insert(SpanBo span) {
		if(null != hbaseTraceDaoV2) {
			hbaseTraceDaoV2.insert(span);
		}
		esTraceDaoV2.insert(span);
	}

	@Override
	public void insertSpanChunk(SpanChunkBo spanChunk) {
		if(null != hbaseTraceDaoV2) {
			hbaseTraceDaoV2.insertSpanChunk(spanChunk);
		}
		esTraceDaoV2.insertSpanChunk(spanChunk);
	}
}
