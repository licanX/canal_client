package com.canal.server.process;

import java.util.List;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.canal.server.component.CanalConnectFactory;
import com.canal.server.util.ThreadPoolUtil;

@Component
public class CanalListener {
	private static final Logger logger = LoggerFactory.getLogger(CanalListener.class);

	@Value("${canal.db.conf}")
	private String canalDbConf;

	@Autowired
	private CanalConnectFactory canalFactory;

	@Autowired
	private CanalConsumer consumer;

	/* 从canal一次读出来的条目数 */
	private static final int CANAL_BATCH_SIZE = 10000;

	/* 读取配置文件中，canal的实例列表 */
	private List<String> canalDbConfList = null;

	/**
	 * 初始化执行
	 *
	 * 2017年9月19日
	 */
	@PostConstruct
	@SuppressWarnings("unchecked")
	private void init() {
		canalDbConfList = (List<String>) JSON.parse(canalDbConf);

		for (String dbConf : canalDbConfList) {
			final String tempConf = dbConf;
			logger.info("启动canal-[" + tempConf + "]监听...");
			ThreadPoolUtil.getThreadPool().execute(new Runnable() {

				@Override
				public void run() {
					listener(tempConf);
				}
			});
		}
	}

	/**
	 * 建立canal多个实例连接，监听多个数据库源
	 *
	 * 2017年9月13日
	 */
	public void listener(String dbConf) {
		long batchId = 0;
		CanalConnector connector = canalFactory.getClusterConnector(dbConf);
		while (true) {
			try {
				connector.connect();
				connector.subscribe(".*\\..*");
				connector.rollback();
				while (true) {
					Message message = connector.getWithoutAck(CANAL_BATCH_SIZE); // 获取指定数量的数据
					batchId = message.getId();
					int size = message.getEntries().size();
					if (batchId == -1 || size == 0) {
						// 200ms 拉一次变动数据,减小cpu使用率
						Thread.sleep(200);
						connector.ack(batchId); // 提交确认
					} else {
						consumer.consumerEntry(message.getEntries());
						connector.ack(batchId); // 提交确认
					}
				}
			} catch (Exception e) {
				logger.error("read canal message error , exception : ", e);
				connector.rollback(batchId); // 处理失败, 回滚数据
			} finally {
				connector.disconnect(); // 关闭连接
			}
		}
	}
}
