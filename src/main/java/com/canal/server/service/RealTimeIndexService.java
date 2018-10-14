package com.canal.server.service;

import java.util.List;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;

import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.canal.server.model.BinlogMessage;
import com.canal.server.model.EsIndexMessage;

/**
 * 索引实时更新接口
 * 
 * @author lic
 * @date 2018年8月30日
 * @since v1.0.0
 */
public interface RealTimeIndexService {

	/**
	 * 提交索引
	 * @param indexReqBuilders
	 * @return boolean
	 * @author lic
	 * @date 2018年8月30日
	 */
	public boolean index(List<IndexRequestBuilder> indexReqBuilders) ;
	
	/**
	 * 更新索引
	 * @param updateRequests
	 * @return boolean
	 * @author lic
	 * @date 2018年8月30日
	 */
	public boolean update(List<UpdateRequest> updateRequests);
	
	/**
	 * 按查询批量更新
	 * @param columns
	 * @param binlog
	 * @return boolean
	 * @author lic
	 * @date 2018年8月30日
	 */
	public boolean updateByQuery(List<Column> columns,BinlogMessage binlog);
	
	/**
	 * 删除索引
	 * @param indexs
	 * @return boolean
	 * @author lic
	 * @date 2018年8月30日
	 */
	public boolean delete(List<EsIndexMessage> indexs);
}
