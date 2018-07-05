package com.navercorp.pinpoint.collector.dao.proxy;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.ApplicationIndexDao;
import com.navercorp.pinpoint.thrift.dto.TAgentInfo;

@Repository("applicationIndexDaoProxy")
public class ApplicationIndexDaoProxy implements ApplicationIndexDao {

	@Autowired(required = false)
	ApplicationIndexDao hbaseApplicationIndexDao;
	
	@Resource
	ApplicationIndexDao esApplicationIndexDao;
	
	@Override
	public void insert(TAgentInfo agentInfo) {
		if(null != hbaseApplicationIndexDao) {
			hbaseApplicationIndexDao.insert(agentInfo);
		}
		esApplicationIndexDao.insert(agentInfo);
	}

}
