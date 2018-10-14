package com.canal.server.process;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.canal.server.component.EsIndexUpdate;
import com.canal.server.model.BinlogMessage;
import com.canal.server.model.EsIndexMessage;
import com.google.common.collect.Lists;

public class UpdateRequestBuilder {
	private static Logger logger = LoggerFactory.getLogger(UpdateRequestBuilder.class);

	/**
	 * 解析index,type,id
	 * 
	 * @param cloumns
	 * @param canalMsg
	 * @return List<IndexEntryDTO>
	 * @author lic
	 * @date 2018年8月30日
	 */
	public static List<EsIndexMessage> parseIndexEntry(List<Column> cloumns, BinlogMessage canalMsg) {
		List<EsIndexMessage> indexEntrys = Lists.newArrayList();
		for (Entry<String, List<String>> indexFieldEntry : canalMsg.getIndexFields().entrySet()) {
			String indexName = indexFieldEntry.getKey();
			EsIndexMessage indexEntry = new EsIndexMessage();
			indexEntry.setIndex(indexName);
			indexEntry.setType(indexName);
			for (Column column : cloumns) {
				if (canalMsg.getPrimaryKey().equals(column.getName())) {
					indexEntry.setId(column.getValue());
				}
			}
			indexEntrys.add(indexEntry);
		}
		return indexEntrys;
	}

	/**
	 * 删除式更新，把对应字段数据清空
	 * 
	 * @param columns
	 * @param canalMsg
	 * @return List<UpdateRequest>
	 * @author lic
	 * @date 2018年8月30日
	 */
	public static List<UpdateRequest> parseUpdelRequest(List<Column> columns, BinlogMessage canalMsg) {
		List<UpdateRequest> updateRequests = Lists.newArrayList();
		List<Column> rebuildColums = EsIndexUpdate.rebuildColums(columns, canalMsg, false);
		if (rebuildColums.isEmpty()) {
			return updateRequests;
		}
		for (Entry<String, List<String>> indexFieldEntry : canalMsg.getIndexFields().entrySet()) {
			String indexName = indexFieldEntry.getKey();
			UpdateRequest updateRequest = new UpdateRequest();
			updateRequest.index(indexName);
			updateRequest.type(indexName);
			try {
				XContentBuilder contentBuilder = jsonBuilder().startObject();
				for (Column column : rebuildColums) {
					if (canalMsg.getPrimaryKey().equals(column.getName())) {
						updateRequest.id(column.getValue());
					} else {
						contentBuilder.field(column.getName(), ""); // 将字段设为空
					}
				}
				contentBuilder.endObject();
				updateRequest.doc(contentBuilder);
				logger.info(updateRequest.id() + "--" + contentBuilder.toString());
				updateRequest.upsert(buildIndexRequest(rebuildColums, canalMsg, indexName));
			} catch (IOException e) {
				e.printStackTrace();
			}
			updateRequests.add(updateRequest);
		}
		return updateRequests;
	}

	/**
	 * upsert方式更新索引
	 * 
	 * IndexRequest indexRequest = new IndexRequest("index", "type", "1")
	 * .source(jsonBuilder() .startObject() .field("name", "Joe Smith")
	 * .field("gender", "male") .endObject()); UpdateRequest updateRequest = new
	 * UpdateRequest("index", "type", "1") .doc(jsonBuilder() .startObject()
	 * .field("gender", "male") .endObject()) .upsert(indexRequest);
	 * client.update(updateRequest).get();
	 *
	 * @param columns
	 * @return 2017t^9g19錯
	 */
	public static List<UpdateRequest> parseUpsertRequest(List<Column> columns, BinlogMessage canalMsg) {
		List<UpdateRequest> updateRequests = Lists.newArrayList();
		List<Column> rebuildColums = rebuildColums(columns, canalMsg);
		if (rebuildColums.isEmpty()) {
			return updateRequests;
		}
		for (Entry<String, List<String>> indexFieldEntry : canalMsg.getIndexFields().entrySet()) {
			String indexName = indexFieldEntry.getKey();
			UpdateRequest updateRequest = new UpdateRequest();
			updateRequest.index(indexName);
			updateRequest.type(indexName);
			try {
				XContentBuilder contentBuilder = jsonBuilder().startObject();
				for (Column column : rebuildColums) {
					if (canalMsg.getPrimaryKey().equals(column.getName())) {
						updateRequest.id(column.getValue());
					}
					if (column.getUpdated() && isDelete(column)) {
						cleanFieldValue(contentBuilder, canalMsg);
					} else if (column.getUpdated() && !"gmt_modified".equals(column.getName())) {
						contentBuilder.field(column.getName(), column.getValue());
					}
				}
				contentBuilder.field("update_time", System.currentTimeMillis() / 1000);
				contentBuilder.endObject();
				logger.info(updateRequest.id() + "--" + contentBuilder.toString());
				updateRequest.doc(contentBuilder).upsert(buildIndexRequest(rebuildColums, canalMsg, indexName));
			} catch (IOException e) {
				e.printStackTrace();
			}
			if (null != updateRequest.id()) {
				updateRequests.add(updateRequest);
			}
		}
		return updateRequests;
	}

	/**
	 * ô暰攏张剤零攌ﾊ打彍剳凨汨裸癳兗宵殄瘼倅湺稍 *
	 * 
	 * @param contentBuilder
	 * @param canalMsg       2018t帱朱6å攍 * @throws IOException
	 */
	private static void cleanFieldValue(XContentBuilder contentBuilder, BinlogMessage canalMsg) throws IOException {
		Map<String, List<String>> indexFieldsMap = canalMsg.getIndexFields();
		if (MapUtils.isNotEmpty(indexFieldsMap)) {
			for (Entry<String, List<String>> entry : indexFieldsMap.entrySet()) {
				List<String> fields = entry.getValue();
				if (CollectionUtils.isNotEmpty(fields)) {
					for (String field : fields) {
						if (isNotDeleteField(field)) {
							contentBuilder.field(field, "");
						}
					}
				}
			}
		}
	}

	/**
	 * °斞堢紕弍 *
	 * 
	 * @param columns
	 * @return 2017t帹朱9å攍
	 */
	private static IndexRequest buildIndexRequest(List<Column> columns, BinlogMessage canalMsg, String indexName) {
		IndexRequest indexRequest = null;
		String docId = "0";
		try {
			XContentBuilder contentBuilder = jsonBuilder().startObject();
			for (Column column : columns) {
				if (canalMsg.getPrimaryKey().equals(column.getName())) {
					docId = column.getValue();
				}
				if (column.getUpdated() && isDelete(column)) {
					cleanFieldValue(contentBuilder, canalMsg);
				} else if (column.getUpdated() && !"gmt_modified".equals(column.getName())) {
					contentBuilder.field(column.getName(), column.getValue());
				}
			}
			contentBuilder.field("update_time", System.currentTimeMillis() / 1000);
			contentBuilder.endObject();
			indexRequest = new IndexRequest(indexName, indexName, docId).source(contentBuilder);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return indexRequest;
	}

	private static List<Column> rebuildColums(List<Column> columns, BinlogMessage canalMsg) {
		List<Column> fieldColumns = Lists.newArrayList();
		Map<String, List<String>> indexFields = canalMsg.getIndexFields();
		List<String> fields = indexFields.entrySet().iterator().next().getValue();
		if (fields.isEmpty()) {
			return columns;
		}
		for (Column column : columns) {
			if ((fields.contains(column.getName()) && column.getUpdated())
					|| column.getName().equals(canalMsg.getPrimaryKey())) {
				fieldColumns.add(column);
			}
		}
		return fieldColumns;
	}

	private static boolean isNotDeleteField(String field) {
		List<String> deleteFields = Lists.newArrayList("is_delete", "status");
		return !deleteFields.contains(field);
	}

	/**
	 * $劭敓彍勴暰敌蠯昦吺业ｩs_delete = 1 || status=0
	 *
	 * @param column
	 * @return 2018t^1g16錯
	 */
	private static boolean isDelete(Column column) {
		boolean isDelete = false;
		String colName = column.getName();
		int colValue = NumberUtils.toInt(column.getValue());
		if (StringUtils.isNotBlank(colName)) {
			isDelete = (("is_delete".equals(colName) && 1 == colValue) || ("status".equals(colName) && 0 == colValue));
		}
		return isDelete;
	}
}
