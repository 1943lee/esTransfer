package com.eztools.estransfer.common;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by lcy on 2018/1/20.
 */
public class ESTargetClient {
	private static final Logger s_logger = LoggerFactory.getLogger(ESTargetClient.class);

	private static RestClient s_LowClient = null;

	private static RestHighLevelClient s_HighClient = null;

	private static ESTargetClient esSourceClient;

	private ESTargetClient(EsTargetConfig esTargetConfig) {
		try
		{
			String[] serverList = esTargetConfig.getEsHosts().split(";");
			String username = esTargetConfig.getEsUserName();
			String password = esTargetConfig.getEsPassword();

			if(null != serverList && serverList.length > 0) {
				HttpHost[] hosts = new HttpHost[serverList.length];
				for (int i = 0; i < serverList.length; i++) {
					String[] ip_port = serverList[i].split(":");
					if (ip_port.length == 2)
					{
						String ip = ComConvert.toString(ip_port[0]);
						int port = ComConvert.toInteger(ip_port[1], 9200);
						hosts[i] = new HttpHost(ip, port, "http");
					}
				}

				final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
				credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
				s_LowClient = RestClient.builder(hosts)
						.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider))
						.build();
				s_HighClient = new RestHighLevelClient(s_LowClient);
				s_logger.info("Elasticsearch连接成功");
			}
			else
			{
				s_logger.error("Elasticsearch服务地址未配置");
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static RestClient getLowClient(EsTargetConfig esTargetConfig) {
		if(esSourceClient == null)
			esSourceClient = new ESTargetClient(esTargetConfig);
		return esSourceClient.s_LowClient;
	}

	public static RestHighLevelClient getHightClient(EsTargetConfig esTargetConfig) {
		if(esSourceClient == null)
			esSourceClient = new ESTargetClient(esTargetConfig);
		return esSourceClient.s_HighClient;
	}
}
