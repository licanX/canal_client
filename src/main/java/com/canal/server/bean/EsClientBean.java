package com.canal.server.bean;

import java.net.InetAddress;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * EsTransportClient初始化
 * @author lic
 * @date 2018年8月30日
 * @since v1.0.0
 */
@Configuration
public class EsClientBean {

	private final static Logger LOG = LoggerFactory.getLogger(EsClientBean.class);

	@Value("${es.client.sniff}")
	private Boolean sniff;

	@Value("${es.cluster.name}")
	private String clusterName;

	@Value("${es.client.host}")
	private String host;

	@Value("${es.client.port}")
	private Integer port;

	private final static String SPLIT = ",";

	@Bean
	public TransportClient initEsTransportClient() {
		if (StringUtils.isBlank(host)) {
			throw new NullPointerException("[initEsTransportClient]es client init fail,host ip is empty;");
		}
		String[] ips = host.split(SPLIT);
		TransportClient client = null;
		try {
			Settings settings = Settings.builder().put("client.transport.sniff", sniff).put("cluster.name", clusterName)
					.build();
			client = new PreBuiltTransportClient(settings);
			for (String ip : ips) {
				client.addTransportAddress(new TransportAddress(InetAddress.getByName(ip), port));
				LOG.info("[initEsTransportClient]add es client success,node info :{}",
						String.format("%s:%d", ip, port));
			}
		} catch (Exception e) {
			LOG.error("[initEsTransportClient]es client init fail,sniff:{},clusterName:{},host:{},port:{}", sniff,
					clusterName, host, port, e);
		}
		return client;
	}

}
