package com.canal.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * 启动类
 * @author lic
 * @date 2018年8月26日
 * @since v1.0.0
 */
@SpringBootApplication
@ComponentScan(basePackages = {"com.canal.server"})
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}
}
