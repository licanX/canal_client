package com.canal.server.component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;

/**
 * 加载配置
 * 
 * @author lic
 * @date 2018年8月30日
 * @since v1.0.0
 */
@Component
public class ConfigCenter {

	private static final String SPLIT = ",";

	/**
	 * 与业务主键能直接关联的数据，字段更新
	 */
	@Value("${focus.primary.fields}")
	private String focusPrimaryFields;
	/**
	 * 不能与业务主键直接关联的数据，updateByQuery方式批量更新
	 */
	@Value("${focus.business.fields}")
	private String focusBizFields;
	
	// Map<dbName.tableName,Map<primaryKey,Map<indexName,List<field>>>
    private static List<Map<String, Map<String, Map<String, List<String>>>>> canalConfs  = null;

    public static void setCanalConfs(List<Map<String, Map<String, Map<String, List<String>>>>> canalConfs) {
    	ConfigCenter.canalConfs = canalConfs;
    }

    public static void setUpdateByQueryConfs(
        List<Map<String, Map<String, Map<String, List<String>>>>> updateByQueryConfs) {
    	ConfigCenter.updateByQueryConfs = updateByQueryConfs;
    }

    private static List<Map<String, Map<String, Map<String, List<String>>>>> updateByQueryConfs = null;

    @SuppressWarnings("unchecked")
    @PostConstruct
    private void initConfig() {
        canalConfs = JSON.parseObject(focusPrimaryFields, List.class);
        updateByQueryConfs = JSON.parseObject(focusBizFields, List.class);
    }

    // Map<indexName,List<Field>>
    public static Map<String, List<String>> getIndexNames(String dbName, String tableName) {
        Map<String, Map<String, List<String>>> keyIndexMap = getKeyIndexMap(dbName, tableName);
        keyIndexMap.putAll(getUpdateKeyIndexMap(dbName, tableName));
        if (MapUtils.isNotEmpty(keyIndexMap)) {
            return keyIndexMap.entrySet().iterator().next().getValue();
        }
        return new HashMap<>();
    }

    public static String getPrimaryKey(String dbName, String tableName) {
        Map<String, Map<String, List<String>>> keyIndexMap = getKeyIndexMap(dbName, tableName);
        if (MapUtils.isNotEmpty(keyIndexMap)) {
            return keyIndexMap.keySet().iterator().next();
        }
        return "";
    }

    public static String getPrimaryKeyByQuery(String dbName, String tableName) {
        Map<String, Map<String, List<String>>> keyIndexMap = getUpdateKeyIndexMap(dbName, tableName);
        if (MapUtils.isNotEmpty(keyIndexMap)) {
            return keyIndexMap.keySet().iterator().next();
        }
        return "";
    }

    public static Map<String, Map<String, List<String>>> getKeyIndexMap(String dbName, String tableName) {
        if (CollectionUtils.isNotEmpty(canalConfs)) {
            canalConfs.addAll(updateByQueryConfs);
            for (Map<String, Map<String, Map<String, List<String>>>> dbConf : canalConfs) {
                String dbTableName = connDbTable(dbName, tableName);
                if (dbConf.get(dbTableName) != null) {
                    return dbConf.get(dbTableName);
                }
            }
        }
        return Maps.newHashMap();

    }

    private static Map<String, Map<String, List<String>>> getUpdateKeyIndexMap(String dbName, String tableName) {
        if (CollectionUtils.isNotEmpty(updateByQueryConfs)) {
            for (Map<String, Map<String, Map<String, List<String>>>> dbConf : updateByQueryConfs) {
                String dbTableName = connDbTable(dbName, tableName);
                if (dbConf.get(dbTableName) != null) {
                    return dbConf.get(dbTableName);
                }
            }
        }
        return new HashMap<>();

    }

    private static String connDbTable(String db, String table) {
        return new StringBuilder().append(db).append(SPLIT).append(table).toString();
    }
}
