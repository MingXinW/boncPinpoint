package com.navercorp.pinpoint.collector.dao.es.base;

import java.net.InetAddress;
import java.net.UnknownHostException;

import javax.annotation.PostConstruct;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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
}
