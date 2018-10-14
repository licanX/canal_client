package com.canal.server.process;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.canal.server.model.BinlogMessage;
import com.canal.server.service.RealTimeIndexService;
import com.google.common.collect.Maps;

@Component
public class CanalEventProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(CanalEventProcessor.class);

	@Autowired
	private RealTimeIndexService indexService;

	@Value("${db.table.field}")
	private String dbTables;

	private Map<String, List<String>> dbTablesMap = Maps.newHashMap();

	@SuppressWarnings("unchecked")
	@PostConstruct
	private void initConfig() {
		dbTablesMap = JSON.parseObject(dbTables, Map.class);
	}

	/**
	 * 按事件决定处理类
	 *
	 * @param canalMsg 2017年9月19日
	 */
	public boolean process(BinlogMessage canalMsg) {
		if (!isMatchDbTable(canalMsg.getDbName(), canalMsg.getTableName())) {
			LOGGER.info("[canalProcess]DB-table:{}-{} isn't focus", canalMsg.getDbName(), canalMsg.getTableName());
			return false;
		}
		Map<String, Map<String, List<String>>> keyIndexMap = canalMsg.getKeyIndexMap();
		for (Entry<String, Map<String, List<String>>> entry : keyIndexMap.entrySet()) {
			canalMsg.setIndexFields(entry.getValue());
			canalMsg.setPrimaryKey(entry.getKey());
			RowChange rowChage = canalMsg.getRowChange();
			EventType eventType = canalMsg.getEventType();
			if (rowChage == null || eventType == null) {
				LOGGER.error("[CanalProcess] RowChage:{} or EventType:{} is null", rowChage, eventType);
				return false;
			}

			for (RowData rowData : rowChage.getRowDatasList()) {
				switch (eventType) {
				case DELETE:
//                            indexService.delete(CanalUpdateRequest.parseIndexEntry(rowData.getBeforeColumnsList(),
//                                                                                   canalMsg));
					indexService
							.update(UpdateRequestBuilder.parseUpdelRequest(rowData.getBeforeColumnsList(), canalMsg));
					break;
				case INSERT:
					indexService
							.update(UpdateRequestBuilder.parseUpsertRequest(rowData.getAfterColumnsList(), canalMsg));
					indexService.updateByQuery(rowData.getAfterColumnsList(), canalMsg);
					break;
				case UPDATE:
					indexService
							.update(UpdateRequestBuilder.parseUpsertRequest(rowData.getAfterColumnsList(), canalMsg));
					indexService.updateByQuery(rowData.getAfterColumnsList(), canalMsg);
					break;
				default:
					break;
				}
			}
		}
		return true;
	}

	private boolean isMatchDbTable(String dbName, String tableName) {
		boolean isClude = false;
		if (dbTablesMap.containsKey(dbName)) {
			List<String> tables = dbTablesMap.get(dbName);
			isClude = tables.contains(tableName);
		}
		return isClude;
	}

	public void setDbTablesMap(Map<String, List<String>> dbTablesMap) {
		this.dbTablesMap = dbTablesMap;
	}
}
