package com.navercorp.pinpoint.collector.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class EsIndexs {
	public String byDay() {
		Date nowDay = new Date();
	    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	    String ndate = dateFormat.format(nowDay);
	    return  ndate;	
	}
	public String byMonth() {
		Date nowMonth = new Date();
	    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM");
	    String ndate = dateFormat.format(nowMonth);
	    return  ndate;
	}
	
	
	public static final String ID_SEP = "&";

	public static final String TYPE = "type";
	
	public static final String AGENT_INFO = "p_agent_info";
	
	public String AGENT_EVENT = "p_agent_event"+"-"+byMonth();
	
	public String AGENT_LIFECYCLE = "p_agent_life_cycle"+"-"+byMonth();
	 
	public static final String API_METADATA = "p_api_meta_data";
	
	public static final String APPLICATION_INDEX = "p_application_index";
	
	public String APPLICATION_TRACE_INDEX = "p_application_trace_index"+"-"+byDay();
	
	public static final String HOST_APPLICATION_MAP_VER2 = "p_host_application_map_ver2";
	
	public static final String STRING_METADATA = "p_string_meta_data";
	//////////////////////////////////////
	public static final String AGENT_STAT = "p_agent_stat";
	
	public String AGENT_STAT_V2 = "p_agent_stat_v2"+"-"+byMonth();
	
	public static final String SQL_META_DATA_VER2 = "p_sql_meta_data_ver2";
	
	public static final String TRACES = "p_traces";
	
	public static final String TRACES_CHUNK = "p_traces_chunk";
	
	public String TRACE_V2 = "p_trace_v2"+"-"+byDay();
	public String TRACE_CHUNK_V2 = "p_trace_chunk_v2"+"-"+byDay();
	
	public String APPLICATION_MAP_STATISTICS_CALLER_VER2 = "p_application_map_statistics_caller_ver2"+"-"+byDay();
	
	public String APPLICATION_MAP_STATISTICS_CALLEE_VER2 = "p_application_map_statistics_callee_ver2"+"-"+byDay();
	
	public String APPLICATION_MAP_STATISTICS_SELF_VER2 = "p_application_map_statistics_self_ver2"+"-"+byDay();
	
	
	//////////////////////////////////////////////
	
	public static final String AGENT_STAT_COL_GC_TYPE = "gcT";
	public static final String AGENT_STAT_COL_GC_OLD_COUNT = "gcOldC";
	public static final String AGENT_STAT_COL_GC_OLD_TIME = "gcOldT";
	public static final String AGENT_STAT_COL_HEAP_USED = "hpU";
	public static final String AGENT_STAT_COL_HEAP_MAX = "hpM";
	public static final String AGENT_STAT_COL_NON_HEAP_USED = "nHpU";
	public static final String AGENT_STAT_COL_NON_HEAP_MAX = "nHpM";
	
	
	public static final String AGENT_STAT_COL_JVM_CPU = "jvmCpu";
	public static final String AGENT_STAT_COL_SYS_CPU = "sysCpu";
	
	public static final String AGENT_STAT_COL_TRANSACTION_SAMPLED_NEW = "tSN";
	public static final String AGENT_STAT_COL_TRANSACTION_SAMPLED_CONTINUATION = "tSC";
	public static final String AGENT_STAT_COL_TRANSACTION_UNSAMPLED_NEW = "tUnSN";
	public static final String AGENT_STAT_COL_TRANSACTION_UNSAMPLED_CONTINUATION = "tUnSC";
	
	public static final String AGENT_STAT_COL_ACTIVE_TRACE_HISTOGRAM = "aH";
	
	public static String buildCallKey(String callerApplicationName, String callerServiceTypeName, String calleeApplicationName,String calleeServiceTypeName) {
		
		String key = callerApplicationName + "^" +callerServiceTypeName+"~"+calleeApplicationName+"^"+calleeServiceTypeName;
		return key;
	}
	
}
