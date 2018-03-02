package com.navercorp.pinpoint.collector.dao.es.stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.collections.map.MultiKeyMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.AgentStatDaoV2;
import com.navercorp.pinpoint.collector.dao.es.base.AgentStatESOperationFactory;
import com.navercorp.pinpoint.common.server.bo.serializer.stat.AgentStatUtils;
import com.navercorp.pinpoint.common.server.bo.stat.AgentStatType;
import com.navercorp.pinpoint.common.server.bo.stat.DataSourceBo;
import com.navercorp.pinpoint.common.server.bo.stat.DataSourceListBo;
import com.navercorp.pinpoint.common.util.CollectionUtils;

@Repository("esDataSourceListDao")
public class ESDataSourceListDao implements AgentStatDaoV2<DataSourceListBo> {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@Override
	public void insert(String agentId, List<DataSourceListBo> dataSourceListBos) {
		// TODO Auto-generated method stub
		if (agentId == null) {
            throw new NullPointerException("agentId must not be null");
        }
        if (CollectionUtils.isEmpty(dataSourceListBos)) {
            return;
        }

        List<DataSourceListBo> reorderedDataSourceListBos = reorderDataSourceListBos(dataSourceListBos);
        
        try {
			AgentStatESOperationFactory.createPuts( agentId, AgentStatType.DATASOURCE, reorderedDataSourceListBos);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("esDataSourceListDao insert error. Cause:{}", e.getMessage(), e);
		}
	}

	  private List<DataSourceListBo> reorderDataSourceListBos(List<DataSourceListBo> dataSourceListBos) {
	        // reorder dataSourceBo using id and timeSlot
	        MultiKeyMap dataSourceListBoMap = new MultiKeyMap();

	        for (DataSourceListBo dataSourceListBo : dataSourceListBos) {
	            for (DataSourceBo dataSourceBo : dataSourceListBo.getList()) {
	                int id = dataSourceBo.getId();
	                long timestamp = dataSourceBo.getTimestamp();
	                long timeSlot = AgentStatUtils.getBaseTimestamp(timestamp);

	                DataSourceListBo mappedDataSourceListBo = (DataSourceListBo) dataSourceListBoMap.get(id, timeSlot);
	                if (mappedDataSourceListBo == null) {
	                    mappedDataSourceListBo = new DataSourceListBo();
	                    mappedDataSourceListBo.setAgentId(dataSourceBo.getAgentId());
	                    mappedDataSourceListBo.setStartTimestamp(dataSourceBo.getStartTimestamp());
	                    mappedDataSourceListBo.setTimestamp(dataSourceBo.getTimestamp());

	                    dataSourceListBoMap.put(id, timeSlot, mappedDataSourceListBo);
	                }

	                // set fastest timestamp
	                if (mappedDataSourceListBo.getTimestamp() > dataSourceBo.getTimestamp()) {
	                    mappedDataSourceListBo.setTimestamp(dataSourceBo.getTimestamp());
	                }

	                mappedDataSourceListBo.add(dataSourceBo);
	            }
	        }

	        Collection values = dataSourceListBoMap.values();
	        return new ArrayList<DataSourceListBo>(values);
	    }
}
