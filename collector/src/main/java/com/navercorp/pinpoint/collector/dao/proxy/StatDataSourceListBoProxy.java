package com.navercorp.pinpoint.collector.dao.proxy;

import java.util.List;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.AgentStatDaoV2;
import com.navercorp.pinpoint.common.server.bo.stat.DataSourceListBo;

@Repository("dataSourceListBoProxy")
public class StatDataSourceListBoProxy implements AgentStatDaoV2<DataSourceListBo> {

	@Autowired(required = false)
	AgentStatDaoV2<DataSourceListBo> hbaseDataSourceListDao;
	
	@Resource
	AgentStatDaoV2<DataSourceListBo> esDataSourceListDao;
	
	@Override
	public void insert(String agentId, List<DataSourceListBo> agentStatDataPoints) {
		if(null != hbaseDataSourceListDao) {
			hbaseDataSourceListDao.insert(agentId, agentStatDataPoints);
		}
		esDataSourceListDao.insert(agentId, agentStatDataPoints);
	}

}
