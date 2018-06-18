package com.eztools.estransfer.common;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Created by lcy on 2018/1/31.
 */
@Component
@ConfigurationProperties(prefix = "app.es.source")
public class EsSourceConfig {
	private String esHosts;
	private String esUserName;
	private String esPassword;
	private String index;
	private String type;
	private String[] properties;
	private String query;

	@Override
	public String toString() {
		return "esSource hosts: " + esHosts + ",index: " + index + ",type: " + type +
				",includes: " + String.join(",", properties) +
				",query: " + getQuery();
	}

	public String getEsHosts() {
		return esHosts;
	}

	public void setEsHosts(String esHosts) {
		this.esHosts = esHosts;
	}

	public String getEsUserName() {
		return esUserName;
	}

	public void setEsUserName(String esUserName) {
		this.esUserName = esUserName;
	}

	public String getEsPassword() {
		return esPassword;
	}

	public void setEsPassword(String esPassword) {
		this.esPassword = esPassword;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String[] getProperties() {
		return properties;
	}

	public void setProperties(String[] properties) {
		this.properties = properties;
	}

	public String getQuery() {
		if(query == null || query.isEmpty()) {
			this.query = "{\"match_all\":{}}";
		}
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}
}
