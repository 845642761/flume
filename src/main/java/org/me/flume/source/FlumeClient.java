package org.me.flume.source;

import java.nio.charset.Charset;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.apache.log4j.Logger;
import org.me.flume.core.log.LogClient;

/**
 * flume客户端
 * 
 * @author: chengbo
 * @date: 2016年1月25日 15:55:56
 * 
 */
public class FlumeClient implements LogClient {
	private Logger log = Logger.getLogger(FlumeClient.class);
	
	private RpcClient client;
	private String hostname;
	private int port;

	public FlumeClient(String hostname, int port) {
		log.info("start create a new flumeClient...");
		this.hostname = hostname;
		this.port = port;
		this.client = RpcClientFactory.getDefaultInstance(hostname, port);
		log.info("create flumeClient complete!");
	}

	@Override
	public boolean log(String data) {
		if(StringUtils.isBlank(data)){
			return true;
		}
		
		Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));
		try {
			client.append(event);
			log.debug("send a log : "+data);
		} catch (EventDeliveryException e) {
			e.printStackTrace();
			log.error(e);
			client = null;
			client = RpcClientFactory.getDefaultInstance(hostname, port);
		    return false;
		}
		return true;
	}

	@Override
	public boolean close() {
		if(client != null){
			try {
				client.close();
			} catch (Exception e) {
				e.printStackTrace();
				log.error(e);
				return false;
			}
		}
		log.info("close a flumeClient!");
		return true;
	}
}
