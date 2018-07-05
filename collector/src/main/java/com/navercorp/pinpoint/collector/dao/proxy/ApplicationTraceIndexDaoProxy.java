package com.navercorp.pinpoint.collector.dao.proxy;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.ApplicationTraceIndexDao;
import com.navercorp.pinpoint.thrift.dto.TSpan;

@Repository("applicationTraceIndexDaoProxy")
public class ApplicationTraceIndexDaoProxy implements ApplicationTraceIndexDao {

	@Resource
	ApplicationTraceIndexDao esApplicationTraceIndexDao;
	
	@Autowired(required = false)
	ApplicationTraceIndexDao hbaseApplicationTraceIndexDao;
	
	@Override
	public void insert(TSpan span) {
		if(null != hbaseApplicationTraceIndexDao) {
			hbaseApplicationTraceIndexDao.insert(span);
		}
		esApplicationTraceIndexDao.insert(span);
	}

}
