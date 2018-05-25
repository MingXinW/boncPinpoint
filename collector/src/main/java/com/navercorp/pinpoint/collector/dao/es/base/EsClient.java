package com.navercorp.pinpoint.collector.dao.es.base;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import javax.annotation.PostConstruct;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.navercorp.pinpoint.collector.util.EsIndexs;

@Component
public class EsClient {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private static TransportClient nativeClient;

	@Value("${es.cluster.name}")
	private String clusterName;

	@Value("${es.cluster.hosts}")
	private String hosts;

	public EsClient() {
		super();
	}

	public EsClient(String clusterName, String hosts) {
		super();
		this.clusterName = clusterName;
		this.hosts = hosts;
	}

	@PostConstruct
	private void initClient() {
		Settings settings = Settings.builder().put("cluster.name", clusterName).build();
		nativeClient = new PreBuiltTransportClient(settings);
		String[] hostsArray = hosts.split(",");
		for (String host : hostsArray) {
			String[] hostAndPort = host.split(":");
			try {
				nativeClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostAndPort[0]),
						Integer.valueOf(hostAndPort[1])));
			} catch (UnknownHostException e) {
				logger.error("es cluster UnknownHost {}", hostAndPort[0]);
			} catch (NumberFormatException e) {
				logger.error("es cluster port error {}", hostAndPort[1]);
			}

		}
	}

	public static TransportClient client() {
		return nativeClient;
	}

	public static <T> IndexResponse insert(T t, String index, String type) throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		String text = mapper.writeValueAsString(t);
		JSONObject jsonbject = JSONObject.parseObject(text);
		jsonbject.put("@timestamp", Long.toString(System.currentTimeMillis()));
		IndexResponse response = EsClient.client().prepareIndex(index, type)
				.setSource(jsonbject.toJSONString(), XContentType.JSON).get();
		return response;
	}
	
	public static <T> IndexResponse insert(T t,String id, String index, String type) throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		String text = mapper.writeValueAsString(t);
		JSONObject jsonbject = JSONObject.parseObject(text);
		jsonbject.put("@timestamp", Long.toString(System.currentTimeMillis()));
		IndexResponse response = EsClient.client().prepareIndex(index, type,id)
				.setSource(jsonbject.toJSONString(), XContentType.JSON).get();
		return response;
	}
	
	public static IndexResponse insert(JSONObject jsonbject,String id, String index, String type) {
		if(jsonbject != null) {
			jsonbject.put("@timestamp", Long.toString(System.currentTimeMillis()));
			IndexResponse response = EsClient.client().prepareIndex(index, type,id)
					.setSource(jsonbject.toJSONString(), XContentType.JSON).get();
			return response;
		}else {
			return null;
		}
	}
	
	public static IndexResponse insert(JSONObject jsonbject,String index, String type) {
		if(jsonbject != null) {
			jsonbject.put("@timestamp", Long.toString(System.currentTimeMillis()));
			IndexResponse response = EsClient.client().prepareIndex(index, type)
					.setSource(jsonbject.toJSONString(), XContentType.JSON).get();
			return response;
		}else {
			return null;
		}
	}
	
	public static long count(String index, String type,QueryBuilder queryBuilder) {
		SearchResponse response = EsClient.client().prepareSearch(index).setTypes(type).setQuery(queryBuilder).get();
		return response.getHits().getTotalHits();
	}
	
	public static boolean indexExists(String index){  
        IndicesExistsRequest request = new IndicesExistsRequest(index);  
        IndicesExistsResponse response = EsClient.client().admin().indices().exists(request).actionGet();  
        if (response.isExists()) {  
            return true;  
        }  
        return false;  
    }  
	
	public static SearchHit[] searh(String index, String type,QueryBuilder queryBuilder) {
		SearchResponse response = EsClient.client().prepareSearch(index).setTypes(type).setQuery(queryBuilder).get();
		SearchHits searchHits  = response.getHits();
		return searchHits.getHits();
	}
	
	public static void update(String index, String type,String id,String field,Object value) throws IOException {
		EsClient.client().prepareUpdate(index, type, id)
        .setDoc(XContentFactory.jsonBuilder()               
            .startObject()
                .field(field, value)
            .endObject())
        .get();
	}
	
	public static void update(UpdateRequest updateRequest) throws InterruptedException, ExecutionException {
		EsClient.client().update(updateRequest).get();
	}
	
	public static void main(String args[]) {
		EsClient esClient = new EsClient("blogic-5.3.3", "172.16.11.128:8300");
		esClient.initClient();
		JSONObject jsonbject = new JSONObject();
		jsonbject.put("@timestamp", System.currentTimeMillis());
		jsonbject.put("test", "test");
		EsClient.insert(jsonbject, "test", "test");
		BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("test", "test"));
		SearchHit[] searchs = EsClient.searh("test", "test", queryBuilder);
		System.out.println("searchs length:"+searchs.length);
		
		
		boolean bool = EsClient.indexExists(EsIndexs.AGENT_INFO);
		System.out.println(bool);
		BoolQueryBuilder queryBuilders = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("agentId", "test-futao")).must(QueryBuilders.matchQuery("startTime", 1520577642401l));
		SearchResponse response = EsClient.client().prepareSearch(EsIndexs.AGENT_INFO).setTypes(EsIndexs.TYPE).setQuery(queryBuilders).get();
		System.out.println(response.getHits().getTotalHits());
		SearchHits searchHits  = response.getHits();
		for(SearchHit hit : searchHits) {
			 Iterator<Entry<String, Object>> iterator = hit.getSource().entrySet().iterator();
             while(iterator.hasNext()) {
                 Entry<String, Object> next = iterator.next();
                 System.out.println(next.getKey() + ": " + next.getValue());
             }
		}
		
	}
}
