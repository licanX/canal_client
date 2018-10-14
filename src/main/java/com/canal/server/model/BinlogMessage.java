package com.canal.server.model;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;

/**
 * binlog消息实体 
 * @author lic
 * @date 2018年8月30日
 * @since v1.0.0
 */
public class BinlogMessage {

	/**
	 * 数据库主键
	 */
	private String primaryKey;
	/**
	 * 数据库名
	 */
	private String dbName;
	/**
	 * 表名
	 */
	private String tableName;
	/**
	 * updateByQuery的业务主键
	 */
	private String primaryKeyByQuery;
	/**
	 * binlog数据实体
	 */
	private RowChange rowChange;
	/**
	 * binlog事件类型
	 */
	private EventType eventType;
	
	/**
	 * Map:索引节点,节点关注字段列表
	 */
	private Map<String,List<String>> indexFields;
	/**
	 * Map:业务主键,indexFeilds
	 */
	private Map<String,Map<String,List<String>>> keyIndexMap;

	public String getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getPrimaryKeyByQuery() {
		return primaryKeyByQuery;
	}

	public void setPrimaryKeyByQuery(String primaryKeyByQuery) {
		this.primaryKeyByQuery = primaryKeyByQuery;
	}

	public RowChange getRowChange() {
		return rowChange;
	}

	public void setRowChange(RowChange rowChange) {
		this.rowChange = rowChange;
	}

	public EventType getEventType() {
		return eventType;
	}

	public void setEventType(EventType eventType) {
		this.eventType = eventType;
	}

	public Map<String, List<String>> getIndexFields() {
		return indexFields;
	}

	public void setIndexFields(Map<String, List<String>> indexFields) {
		this.indexFields = indexFields;
	}

	public Map<String, Map<String, List<String>>> getKeyIndexMap() {
		return keyIndexMap;
	}

	public void setKeyIndexMap(Map<String, Map<String, List<String>>> keyIndexMap) {
		this.keyIndexMap = keyIndexMap;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE);
	}
	
}
