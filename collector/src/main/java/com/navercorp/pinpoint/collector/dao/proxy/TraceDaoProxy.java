package com.navercorp.pinpoint.collector.dao.proxy;

import javax.annotation.Resource;

import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.TraceDao;
import com.navercorp.pinpoint.common.server.bo.SpanBo;
import com.navercorp.pinpoint.common.server.bo.SpanChunkBo;

@Repository("traceDaoProxy")
public class TraceDaoProxy implements TraceDao {

	@Resource
	private TraceDao esTraceDao;
	
	@Resource
	private TraceDao hbaseTraceDao;
	
	@Override
	public void insert(SpanBo span) {
		// TODO Auto-generated method stub
		hbaseTraceDao.insert(span);
		esTraceDao.insert(span);
	}

	@Override
	public void insertSpanChunk(SpanChunkBo spanChunk) {
		// TODO Auto-generated method stub
		hbaseTraceDao.insertSpanChunk(spanChunk);
		esTraceDao.insertSpanChunk(spanChunk);
	}

}
