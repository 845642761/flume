package org.me.flume.core.log;

public interface LogClient {
	/**
	 * 记录日志
	 * @param: data
	 * @return: boolean
	 * @author: chengbo
	 * @date: 2016年1月25日 17:17:22
	 */
	public boolean log(String data);
	
	/**
	 * 关闭客户端连接
	 * @author: chengbo
	 * @date: 2016年1月25日 17:17:51
	 */
	public boolean close();
}
