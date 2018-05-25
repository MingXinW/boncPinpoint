package com.navercorp.pinpoint.collector.dao.proxy;

import javax.annotation.Resource;

import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.ApplicationTraceIndexDao;
import com.navercorp.pinpoint.thrift.dto.TSpan;

@Repository("applicationTraceIndexDaoProxy")
public class ApplicationTraceIndexDaoProxy implements ApplicationTraceIndexDao {

	@Resource
	ApplicationTraceIndexDao esApplicationTraceIndexDao;
	
	@Resource
	ApplicationTraceIndexDao hbaseApplicationTraceIndexDao;
	
	@Override
	public void insert(TSpan span) {
		// TODO Auto-generated method stub

		hbaseApplicationTraceIndexDao.insert(span);
		esApplicationTraceIndexDao.insert(span);
	}

}
