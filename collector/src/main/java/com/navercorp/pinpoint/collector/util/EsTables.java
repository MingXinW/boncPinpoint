package com.navercorp.pinpoint.collector.util;

import com.navercorp.pinpoint.common.PinpointConstants;

public class EsTables {

	 	public static final int APPLICATION_NAME_MAX_LEN = PinpointConstants.APPLICATION_NAME_MAX_LEN;
	    public static final int AGENT_NAME_MAX_LEN = PinpointConstants.AGENT_NAME_MAX_LEN;


	    public static final String APPLICATION_TRACE_INDEX = "applicationtraceindex";
	    public static final String APPLICATION_TRACE_INDEX_CF_TRACE = "i"; // applicationIndex
	    public static final int APPLICATION_TRACE_INDEX_ROW_DISTRIBUTE_SIZE = 1; // applicationIndex hash size

	    public static final String AGENT_STAT = "agentstat";
	    public static final String AGENT_STAT_CF_STATISTICS = "s"; // agent statistics column family
	    public static final String AGENT_STAT_COL_INTERVAL = "int"; // qualifier for collection interval
	    public static final String AGENT_STAT_COL_GC_TYPE = "gct"; // qualifier for GC type
	    public static final String AGENT_STAT_COL_GC_OLD_COUNT = "gcoldc"; // qualifier for GC old count
	    public static final String AGENT_STAT_COL_GC_OLD_TIME = "gcoldt"; // qualifier for GC old time
	    public static final String AGENT_STAT_COL_HEAP_USED = "hpu"; // gualifier for heap used
	    public static final String AGENT_STAT_COL_HEAP_MAX = "hpm"; // qualifier for heap max
	    public static final String AGENT_STAT_COL_NON_HEAP_USED = "nhpu"; // qualifier for non-heap used
	    public static final String AGENT_STAT_COL_NON_HEAP_MAX = "nhpm"; // qualifier for non-heap max
	    public static final String AGENT_STAT_COL_JVM_CPU = "jvmcpu"; // qualifier for JVM CPU usage
	    public static final String AGENT_STAT_COL_SYS_CPU = "syscpu"; // qualifier for system CPU usage
	    public static final String AGENT_STAT_COL_TRANSACTION_SAMPLED_NEW = "tsn"; // qualifier for sampled new count
	    public static final String AGENT_STAT_COL_TRANSACTION_SAMPLED_CONTINUATION = "tsc"; // qualifier for sampled continuation count
	    public static final String AGENT_STAT_COL_TRANSACTION_UNSAMPLED_NEW = "tunsn"; // qualifier for unsampled new count
	    public static final String AGENT_STAT_COL_TRANSACTION_UNSAMPLED_CONTINUATION = "tunsc"; // qualifier for unsampled continuation count
	    public static final String AGENT_STAT_COL_ACTIVE_TRACE_HISTOGRAM = "ah"; // qualifier for active trace histogram
	    public static final int AGENT_STAT_ROW_DISTRIBUTE_SIZE = 1; // agent statistics hash size

	    public static final String TRACES = "traces";
	    public static final String TRACES_CF_SPAN = "s";  //Span
	    public static final String TRACES_CF_ANNOTATION = "a";  //Annotation
	    public static final String TRACES_CF_TERMINALSPAN = "t"; //TerminalSpan

	    public static final String APPLICATION_INDEX = "applicationindex";
	    public static final String APPLICATION_INDEX_CF_AGENTS = "agents";

	    public static final String AGENTINFO = "agentinfo";
	    public static final String AGENTINFO_CF_INFO = "info";
	    public static final String AGENTINFO_CF_INFO_IDENTIFIER = "i";
	    public static final String AGENTINFO_CF_INFO_SERVER_META_DATA = "m";
	    public static final String AGENTINFO_CF_INFO_JVM = "j";

	    public static final String AGENT_LIFECYCLE = "agentlifecycle";
	    public static final String AGENT_LIFECYCLE_CF_STATUS = "s"; // agent lifecycle column family
	    public static final String AGENT_LIFECYCLE_CF_STATUS_QUALI_STATES = "states"; // qualifier for agent lifecycle states

	    public static final String AGENT_EVENT = "agentevent";
	    public static final String AGENT_EVENT_CF_EVENTS = "e"; // agent events column family


	    public static final String SQL_METADATA_VER2 = "sqlmetadata_ver2";
	    public static final String SQL_METADATA_VER2_CF_SQL = "sql";
	    public static final String SQL_METADATA_VER2_CF_SQL_QUALI_SQLSTATEMENT = "p_sql_statement";

	    public static final String STRING_METADATA = "stringmetadata";
	    public static final String STRING_METADATA_CF_STR = "str";
	    public static final String STRING_METADATA_CF_STR_QUALI_STRING = "p_string";

	    public static final String API_METADATA = "apimetadata";
	    public static final String API_METADATA_CF_API = "api";
	    public static final String API_METADATA_CF_API_QUALI_SIGNATURE = "p_api_signature";

	    public static final String MAP_STATISTICS_CALLER_VER2 = "applicationmapstatisticscaller_ver2";
	    public static final String MAP_STATISTICS_CALLER_VER2_CF_COUNTER = "c";

	    public static final String MAP_STATISTICS_CALLEE_VER2 = "applicationmapstatisticscallee_ver2";
	    public static final String MAP_STATISTICS_CALLEE_VER2_CF_COUNTER = "c";


	    public static final String MAP_STATISTICS_SELF_VER2 = "applicationmapstatisticsself_ver2";
	    public static final String MAP_STATISTICS_SELF_VER2_CF_COUNTER = "c";

	    public static final String HOST_APPLICATION_MAP_VER2 = "hostapplicationmap_ver2";
	    public static final String HOST_APPLICATION_MAP_VER2_CF_MAP = "m";
}
