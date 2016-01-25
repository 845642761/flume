package org.me.flume.source;

import org.apache.log4j.Logger;

public class Source_Test {
	private Logger log = Logger.getLogger(Source_Test.class);
	
	public static void main(String[] args) {
		Source_Test st = new Source_Test();
		int i = 0;
		while (true) {
			st.log("log line num = "+i);
			i++;
			st.sleep(1000);
		}
	}
	
	/**
	 * 记录日志
	 * @author: chengbo
	 * @date: 2016年1月13日 11:16:36
	 */
	private void log(String str) {
		log.debug(str);
	}
	
	/**
	 * 设置休眠
	 * @date: 2016年1月13日 11:19:26
	 * @author: chengbo
	 */
	private void sleep(long times) {
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
