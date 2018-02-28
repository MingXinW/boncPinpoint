package com.navercorp.pinpoint.collector.dao.es.stat;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.AgentStatDao;
import com.navercorp.pinpoint.collector.dao.es.base.EsClient;
import com.navercorp.pinpoint.collector.mapper.thrift.ActiveTraceHistogramBoMapper;
import com.navercorp.pinpoint.collector.util.EsIndexs;
import com.navercorp.pinpoint.common.server.bo.ActiveTraceHistogramBo;
import com.navercorp.pinpoint.common.server.bo.serializer.stat.AgentStatUtils;
import com.navercorp.pinpoint.thrift.dto.TActiveTrace;
import com.navercorp.pinpoint.thrift.dto.TAgentStat;
import com.navercorp.pinpoint.thrift.dto.TCpuLoad;
import com.navercorp.pinpoint.thrift.dto.TJvmGc;
import com.navercorp.pinpoint.thrift.dto.TJvmGcType;
import com.navercorp.pinpoint.thrift.dto.TTransaction;

@Repository("esAgentStatDao")
@Deprecated
public class ESAgentStatDao implements AgentStatDao {

	@Autowired
    private ActiveTraceHistogramBoMapper activeTraceHistogramBoMapper;
	
	@Override
	public void insert(TAgentStat agentStat) {
		// TODO Auto-generated method stub
		if (agentStat == null) {
			throw new NullPointerException("agentStat must not be null");
		}
		createPut(agentStat);
		
	}
	
	private void createPut(TAgentStat agentStat) {
        long timestamp = agentStat.getTimestamp();
        String id = agentStat.getAgentId() + EsIndexs.ID_SEP + timestamp;
        
        TransportClient client = EsClient.client();
        
       
        try {
        	
	        XContentBuilder xcontentBuilder = jsonBuilder().startObject();
	        
	        final long collectInterval = agentStat.getCollectInterval();
	        xcontentBuilder.field("collectInterval", collectInterval);
	        
	        
	        // GC, Memory
	        if (agentStat.isSetGc()) {
	            TJvmGc gc = agentStat.getGc();
	            xcontentBuilder.field(EsIndexs.AGENT_STAT_COL_GC_TYPE, gc.getType().name());
	            xcontentBuilder.field(EsIndexs.AGENT_STAT_COL_GC_OLD_COUNT, gc.getJvmGcOldCount());
	            xcontentBuilder.field(EsIndexs.AGENT_STAT_COL_GC_OLD_TIME, gc.getJvmGcOldTime());
	            xcontentBuilder.field(EsIndexs.AGENT_STAT_COL_HEAP_USED, gc.getJvmMemoryHeapUsed());
	            xcontentBuilder.field(EsIndexs.AGENT_STAT_COL_HEAP_MAX, gc.getJvmMemoryHeapMax());
	            xcontentBuilder.field(EsIndexs.AGENT_STAT_COL_NON_HEAP_USED, gc.getJvmMemoryNonHeapUsed());
	            xcontentBuilder.field(EsIndexs.AGENT_STAT_COL_NON_HEAP_MAX, gc.getJvmMemoryNonHeapMax());
	            
	            
	        } else {
	            
	            xcontentBuilder.field(EsIndexs.AGENT_STAT_COL_GC_TYPE, TJvmGcType.UNKNOWN.name());
	        }
	        // CPU
	        if (agentStat.isSetCpuLoad()) {
	            TCpuLoad cpuLoad = agentStat.getCpuLoad();
	            double jvmCpuLoad = AgentStatUtils.convertLongToDouble(AgentStatUtils.convertDoubleToLong(cpuLoad.getJvmCpuLoad()));
	            double systemCpuLoad = AgentStatUtils.convertLongToDouble(AgentStatUtils.convertDoubleToLong(cpuLoad.getSystemCpuLoad()));
	            xcontentBuilder.field(EsIndexs.AGENT_STAT_COL_JVM_CPU, jvmCpuLoad);
	            xcontentBuilder.field(EsIndexs.AGENT_STAT_COL_SYS_CPU, systemCpuLoad);
	        }
	        // Transaction
	        if (agentStat.isSetTransaction()) {
	            TTransaction transaction = agentStat.getTransaction();
	            
	            xcontentBuilder.field(EsIndexs.AGENT_STAT_COL_TRANSACTION_SAMPLED_NEW, transaction.getSampledNewCount());
	            xcontentBuilder.field(EsIndexs.AGENT_STAT_COL_TRANSACTION_SAMPLED_CONTINUATION, transaction.getSampledContinuationCount());
	            xcontentBuilder.field(EsIndexs.AGENT_STAT_COL_TRANSACTION_UNSAMPLED_NEW, transaction.getUnsampledNewCount());
	            xcontentBuilder.field(EsIndexs.AGENT_STAT_COL_TRANSACTION_UNSAMPLED_CONTINUATION, transaction.getUnsampledContinuationCount());
	        }
	        // Active Trace
	        if (agentStat.isSetActiveTrace()) {
	            TActiveTrace activeTrace = agentStat.getActiveTrace();
	            if (activeTrace.isSetHistogram()) {
	                ActiveTraceHistogramBo activeTraceHistogramBo = this.activeTraceHistogramBoMapper.map(activeTrace.getHistogram());
	                xcontentBuilder.field(EsIndexs.AGENT_STAT_COL_ACTIVE_TRACE_HISTOGRAM, activeTraceHistogramBo);
	            }
	        }
	        client.prepareIndex(EsIndexs.AGENT_STAT, EsIndexs.TYPE, id).setSource(xcontentBuilder.endObject()).get();
        } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

}
