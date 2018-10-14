package com.canal.server.util;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

/**
 * 构造线程池
 * 
 * @author lican
 * @date 2018年8月28日
 * @since v1.0.0
 */
public class ThreadPoolUtil {

	/**
	 * 处理器 可用核数
	 */
	private static final int CPU_COUNT = Runtime.getRuntime().availableProcessors();

	/**
	 * 最大阻塞队列
	 */
	private static final int MAX_QUEUE_SIZE = 128;

	/**
	 * 线程池维护线程的最小数量
	 */
	private static final int CORE_POOL_SIZE = CPU_COUNT * 4;

	/**
	 * 线程池维护线程的最大数量
	 */
	private static final int MAXIMUM_POOL_SIZE = CORE_POOL_SIZE * 4;

	/**
	 * 线程池维护线程所允许的空闲时间
	 */
	private static final int KEEP_ALIVE_TIME = 60;

	/**
	 * 时间单位
	 */
	private static final TimeUnit UNIT = TimeUnit.SECONDS;

	/**
	 * 线程池初始化
	 */
	private static ThreadPoolExecutor threadPool = new ThreadPoolExecutor(CORE_POOL_SIZE, MAXIMUM_POOL_SIZE,
			KEEP_ALIVE_TIME, UNIT, new ArrayBlockingQueue<>(MAX_QUEUE_SIZE),new CallerRunsPolicy());

	/**
	 * 获取线程池
	 * 
	 * @return 
	 */
	public static ThreadPoolExecutor getThreadPool() {
		return threadPool;
	}
}
