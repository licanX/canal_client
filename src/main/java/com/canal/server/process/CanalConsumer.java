package com.canal.server.process;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.canal.server.component.ConfigCenter;
import com.canal.server.model.BinlogMessage;
import com.canal.server.util.ThreadPoolUtil;

@Component
public class CanalConsumer {
	@Autowired
	private CanalEventProcessor eventProcess;

	/**
	 * 消费来自数据库变更的消息
	 *
	 * @param entrys 2017年9月13日
	 */
	public boolean consumerEntry(List<Entry> entrys) {
		for (Entry entry : entrys) {
			if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN
					|| entry.getEntryType() == EntryType.TRANSACTIONEND) {
				continue;
			}
			RowChange rowChage = null;
			try {
				rowChage = RowChange.parseFrom(entry.getStoreValue());
			} catch (Exception e) {
				throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
						e);
			}
			String dbName = entry.getHeader().getSchemaName();
			String tableName = entry.getHeader().getTableName();
			BinlogMessage canalMsg = new BinlogMessage();
			canalMsg.setEventType(rowChage.getEventType());
			canalMsg.setDbName(dbName);
			canalMsg.setTableName(tableName);
			canalMsg.setKeyIndexMap(ConfigCenter.getKeyIndexMap(dbName, tableName));
			canalMsg.setPrimaryKeyByQuery(ConfigCenter.getPrimaryKeyByQuery(dbName, tableName));
			canalMsg.setRowChange(rowChage);
			ThreadPoolUtil.getThreadPool().execute(new Runnable() {

				@Override
				public void run() {

					eventProcess.process(canalMsg);
				}
			});
		}
		return true;
	}
}
