package com.canal.server.component;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.canal.server.model.BinlogMessage;
import com.google.common.collect.Lists;

/**
 * Http json请求es
 * @author lic
 * @date 2018年8月30日
 * @since v1.0.0
 */
@Component
public class EsIndexUpdate {

	private static final String SPLIT = ",";
	
	@Value("${es.client.host}")
	private String esHost;
	@Value("${es.client.port}")
	private int esPort;
	
	/**
	 * 获取可用的Es集群列表
	 * @param index
	 * @return List<String>
	 * @author lic
	 * @date 2018年8月30日
	 */
	public List<String> getEsUrls(String index){
		if(StringUtils.isBlank(index)||StringUtils.isBlank(esHost)) {
			return Lists.newArrayList();
		}
		List<String> requestUrls = Lists.newArrayList();
		String[] hostIps = esHost.split(SPLIT);
		for(String ip : hostIps) {
			String url = "http://"+ip + ":" + esPort + "/" + index+"/_update_by_query";
			requestUrls.add(url);
		}
		return requestUrls;
	}
	/**
     * 只获取关注的字段列表
     *
     * @param columns
     * @param canalMsg
     * @return
     * 2018年1月12日
     */
    public static List<Column> rebuildColums(List<Column> columns, BinlogMessage canalMsg, boolean isUpdateByQuery) {
        List<Column> fieldColumns = Lists.newArrayList();
        Map<String, List<String>> indexFields = canalMsg.getIndexFields();
        List<String> fields = indexFields.entrySet().iterator().next().getValue();
        if (fields.isEmpty()) {
            return columns;
        }
        for (Column column : columns) {
            String primaryKey = canalMsg.getPrimaryKey();
            if (isUpdateByQuery) {
                primaryKey = canalMsg.getPrimaryKeyByQuery();
            }
            if ((fields.contains(column.getName()) && column.getUpdated()) || column.getName().equals(primaryKey)) {
                fieldColumns.add(column);
            }
        }
        return fieldColumns;
    }
}
