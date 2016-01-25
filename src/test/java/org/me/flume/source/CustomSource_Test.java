package org.me.flume.source;

/**
 * 自定义Source测试
 * @author: chengbo
 * @date: 2016年1月25日 17:29:52
 */
public class CustomSource_Test {

	public static void main(String[] args) {
		String hostname = "10.10.8.217";
		int port = 44444;
		FlumeClient fc = new FlumeClient(hostname, port);
		for (int i = 0; i < 20; i++) {
			fc.log(i+"");
		}
		fc.close();
	}
}
