package com.canal.server.bean;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import feign.Client;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;

/**
 * okHttpClient 初始化
 * 
 * @author lic
 * @date 2018年8月30日
 * @since v1.0.0
 */
@Configuration
@ConditionalOnMissingBean({ OkHttpClient.class, Client.class })
public class OkHttpClientBean {

	@Value("${feign.http.connectTimeoutMillis:2000}")
	private int connectTimeoutMillis;
	@Value("${feign.http.readTimeoutMillis:2000}")
	private int readTimeoutMillis;
	@Value("${feign.http.retryCount:3}")
	private int retryCount;
	@Value("${feign.http.retryInterval:1000}")
	private int retryInterval;

	@Bean
	@ConditionalOnMissingBean({ OkHttpClient.class })
	public OkHttpClient okHttpClient() {
		return new OkHttpClient.Builder().retryOnConnectionFailure(true).connectionPool(new ConnectionPool())
				.connectTimeout(connectTimeoutMillis, TimeUnit.MILLISECONDS)
				.readTimeout(readTimeoutMillis, TimeUnit.MILLISECONDS).build();
	}
}
