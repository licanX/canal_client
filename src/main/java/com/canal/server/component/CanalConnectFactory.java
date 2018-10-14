package com.canal.server.component;

import org.springframework.stereotype.Component;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;

import java.net.InetSocketAddress;

import org.springframework.beans.factory.annotation.Value;

/**
 * canal服务器连接工厂
 * 
 * @author lic
 * @date 2018年8月30日
 * @since v1.0.0
 */
@Component
public class CanalConnectFactory {

	@Value("${canal.ip}")
	private String canalIp;

	@Value("${canal.port}")
	private Integer canalPort;

	@Value("${canal.zookeeper.host}")
	private String zkHost;

	/**
	 * zookeeper集群模式连接 对应canal服务器配置为集群模式
	 * 
	 * @param dbConf
	 * @return CanalConnector
	 * @author lic
	 * @date 2018年8月30日
	 */
	public CanalConnector getClusterConnector(String dbConf) {
		return CanalConnectors.newClusterConnector(zkHost, dbConf, "", "");
	}

	/**
	 * 单机版模式连接
	 * 
	 * @param dbConf
	 * @return CanalConnector
	 * @author lic
	 * @date 2018年8月30日
	 */
	public CanalConnector getCanalConnector(String dbConf) {
		return CanalConnectors.newSingleConnector(new InetSocketAddress(canalIp, canalPort), dbConf, "", "");
	}

	public void ack(CanalConnector connector, long batchId) {
		connector.ack(batchId);
	}
}
