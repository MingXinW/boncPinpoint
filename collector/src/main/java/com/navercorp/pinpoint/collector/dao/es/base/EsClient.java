package com.navercorp.pinpoint.collector.dao.es.base;

import java.net.InetAddress;
import java.net.UnknownHostException;

import javax.annotation.PostConstruct;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class EsClient {

	private static final Logger LOG = LoggerFactory.getLogger(EsClient.class);

	private static TransportClient nativeClient;

	@Value("${es.cluster.name}")
	private String clusterName;

	@Value("${es.cluster.hosts}")
	private String hosts;

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
				LOG.error("es cluster UnknownHost {}", hostAndPort[0]);
			} catch (NumberFormatException e) {
				LOG.error("es cluster port error {}", hostAndPort[1]);
			}

		}
	}

	public static TransportClient client() {
		return nativeClient;
	}

}
