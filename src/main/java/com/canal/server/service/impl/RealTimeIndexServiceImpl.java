package com.canal.server.service.impl;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.canal.server.component.EsIndexUpdate;
import com.canal.server.component.OkHttpClientUtil;
import com.canal.server.model.BinlogMessage;
import com.canal.server.model.EsIndexMessage;
import com.canal.server.service.RealTimeIndexService;
import com.google.common.collect.Lists;

/**
 * 实时索引更新接口
 * 
 * @author lic
 * @date 2018年8月30日
 * @since v1.0.0
 */
@Service
public class RealTimeIndexServiceImpl implements RealTimeIndexService {

	private static final Logger LOGGER = LoggerFactory.getLogger(RealTimeIndexServiceImpl.class);

	@Autowired
	private TransportClient esClient;
	@Autowired
	private EsIndexUpdate updateHelper;

	@Autowired
	private OkHttpClientUtil httpClient;

	@Override
	public boolean index(List<IndexRequestBuilder> indexReqsBuilders) {
		if (CollectionUtils.isEmpty(indexReqsBuilders)) {
			LOGGER.error("[index]indexReqsBuilders is empty");
			return false;
		}
		BulkRequestBuilder bulkRequest = esClient.prepareBulk();
		for (IndexRequestBuilder builder : indexReqsBuilders) {
			bulkRequest.add(builder);
		}
		BulkResponse bulkResponse = bulkRequest.get();
		if (bulkResponse.hasFailures()) {
			LOGGER.error("有失败的索引构建：{},indexBuilders:{}", bulkResponse.buildFailureMessage(), indexReqsBuilders);
			return false;
		}
		return true;
	}

	@Override
	public synchronized boolean update(List<UpdateRequest> updateRequests) {
		if (CollectionUtils.isEmpty(updateRequests)) {
			LOGGER.error("[update]updateRequests is empty");
			return false;
		}
		for (UpdateRequest updateRequest : updateRequests) {
			try {
				esClient.update(updateRequest).get();
			} catch (InterruptedException e) {
				LOGGER.error("索引更新失败updateRequest:{}，发生InterruptedException异常：", updateRequests, e);
			} catch (ExecutionException e) {
				LOGGER.error("索引更新失败updateRequest:{}，发生ExecutionException异常：", updateRequests, e);
			}
		}
		return true;
	}

	@Override
	public boolean updateByQuery(List<Column> columns, BinlogMessage canalMsg) {
		if (CollectionUtils.isEmpty(columns) || null == canalMsg) {
			LOGGER.error("[updateByQuery]columns:{} or canalMsg:{},is empty", columns, canalMsg);
			return false;
		}
		String primaryKeyByQuery = canalMsg.getPrimaryKeyByQuery();
		List<Column> rebuildColums = EsIndexUpdate.rebuildColums(columns, canalMsg, true);
		if (CollectionUtils.isEmpty(rebuildColums) || StringUtils.isBlank(primaryKeyByQuery)) {
			LOGGER.info("[updateByQuery]rebuildColums:{} or primaryKeyByQuery:{},is empty", rebuildColums,
					primaryKeyByQuery);
			return false;
		}
		for (Entry<String, List<String>> indexFieldEntry : canalMsg.getIndexFields().entrySet()) {
			String indexName = indexFieldEntry.getKey();
			List<String> queryBuilders = Lists.newArrayList();
			List<String> scriptBuilders = Lists.newArrayList();
			for (Column column : rebuildColums) {
				if (StringUtils.isNotBlank(primaryKeyByQuery) && primaryKeyByQuery.equals(column.getName())
						&& StringUtils.isNotBlank(column.getValue())) {
					String queryBuilder = queryBuilder(column.getName(), column.getValue());
					queryBuilders.add(queryBuilder);
				} else if (column.getUpdated() && !"gmt_modified".equals(column.getName())
						&& StringUtils.isNotBlank(column.getValue())) {
					String scriptBuilder = scriptBuilder(column.getName(), column.getValue());
					scriptBuilders.add(scriptBuilder);
				}
			}
			String requestJson = updateByQueryBuilder(queryBuilders, scriptBuilders);
			LOGGER.info("updateByQuery canal msg : " + requestJson);
			List<String> urls = updateHelper.getEsUrls(indexName);
			boolean success = httpClient.sendPosts(urls, requestJson);
			if (!success) {
				LOGGER.error("update by query error,requestJson:" + requestJson);
			}
		}
		return true;
	}

	/**
	 * ctx._source.suspect_no='yy';
	 */
	private String scriptBuilder(String field, String value) {
		return new StringBuilder().append("ctx._source.").append(field).append("=\'").append(value).append("\';")
				.toString();
	}

	/** "query":{"term": {"idcard_no": "522122197212301665"}} */
	private String queryBuilder(String field, String value) {
		return new StringBuilder().append("\"query\": {\"term\": {\"").append(field).append("\":\"")
				.append(value.toLowerCase()).append("\"}}").toString();
	}

	/**
	 * {"query":{ "term": { "idcard_no": "522122197212301665" } }, "script": {
	 * "inline": "ctx._source.suspect_name='02';ctx._source.suspect_name='xx'",
	 * "lang": "painless" } }
	 */
	private String updateByQueryBuilder(List<String> queryBuilders, List<String> scriptBuilders) {
		long updateTime = System.currentTimeMillis() / 1000;
		StringBuilder sb = new StringBuilder("{");
		if (CollectionUtils.isNotEmpty(queryBuilders) && CollectionUtils.isNotEmpty(scriptBuilders)) {
			for (String queryBuilder : queryBuilders) {
				sb.append(queryBuilder);
			}
			sb.append(",");
			sb.append("\"script\": {\"inline\": \"ctx._source.update_time='" + updateTime + "';");
			for (String scriptBuilder : scriptBuilders) {
				sb.append(scriptBuilder);
			}
			sb.append("\",");
			sb.append("\"lang\":\"painless\"}");
		}
		sb.append("}");
		LOGGER.info("update by query request json : " + sb.toString());
		return sb.toString();

	}

	@Override
	public boolean delete(List<EsIndexMessage> indexEntrys) {
		if (CollectionUtils.isEmpty(indexEntrys)) {
			LOGGER.error("[deleteIndexEntry]entrys is empty");
			return false;
		}
		for (EsIndexMessage indexEntry : indexEntrys) {
			if (indexEntry != null && indexEntry.getId() != null) {
				esClient.prepareDelete(indexEntry.getIndex(), indexEntry.getType(), indexEntry.getId()).execute()
						.actionGet();
				LOGGER.info("索引删除成功，信息：" + indexEntry.toString());
			}
		}
		return true;
	}
}
