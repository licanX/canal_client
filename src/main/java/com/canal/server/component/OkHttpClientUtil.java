package com.canal.server.component;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import okhttp3.Call;
import okhttp3.FormBody;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/**
 * okHttpClient util
 * 
 * @author lic
 * @date 2018年8月30日
 * @since v1.0.0
 */
@Component
public class OkHttpClientUtil {

	private static final Logger LOG = LoggerFactory.getLogger(OkHttpClientUtil.class);

	@Autowired
	private OkHttpClient client;

	/**
	 * Http Get
	 * 
	 * @param url
	 * @return boolean
	 * @author lic
	 * @date 2018年8月30日
	 */
	public String get(String url) {
		Request request = new Request.Builder().get().url(url).build();
		Call call = client.newCall(request);
		Response resp = null;
		try {
			resp = call.execute();
			LOG.info("[GET]request url:{},response:{}", url, resp);
			if (null == resp || 200 != resp.code() || StringUtils.isBlank(resp.body().string())) {
				LOG.warn("[GET]response is error,url:{},response:{}", url, resp);
				return StringUtils.EMPTY;
			}
			return resp.body().string();
		} catch (Exception e) {
			LOG.error("[GET]request url:{}", url, e);
			return StringUtils.EMPTY;
		}
	}

	/**
	 * Http Post
	 * 
	 * @param url
	 * @param json
	 * @return String
	 * @author lic
	 * @date 2018年8月30日
	 */
	public String post(String url, String json) {
		RequestBody requestBody = FormBody.create(MediaType.parse("application/json; charset=utf-8"), json);
		Request request = new Request.Builder().post(requestBody).url(url).build();
		Call call = client.newCall(request);
		Response resp = null;
		try {
			resp = call.execute();
			LOG.info("[POST]request url:{},param:{},response:{}", url, json, resp);
			if (null == resp || 200 != resp.code() || StringUtils.isBlank(resp.body().string())) {
				LOG.warn("[POST]response is error,url:{},param:{},response:{}", url, json, resp);
				return StringUtils.EMPTY;
			}
			return resp.body().string();
		} catch (Exception e) {
			LOG.error("[POST]request url:{},param:{}", url, json, e);
			return StringUtils.EMPTY;
		}
	}

	/**
	 * 多个Post请求，成功一个即可
	 * 
	 * @param urls
	 * @param json
	 * @return boolean
	 * @author lic
	 * @date 2018年8月30日
	 */
	public boolean sendPosts(List<String> urls, String json) {
		if (CollectionUtils.isEmpty(urls) || StringUtils.isBlank(json)) {
			LOG.error("[sendPosts]urls:{},json:{},is empty", urls, json);
			return false;
		}
		LOG.info("[sendPosts]send post ready,urls:{},json:{}", urls, json);
		for (String url : urls) {
			if (StringUtils.isNotBlank(post(url, json))) {
				LOG.info("[sendPosts]send post success,url:{}", url, json);
				return true;
			}
			LOG.error("[sendPosts]send post fail,url:{}", url, json);
		}
		return false;
	}
}
