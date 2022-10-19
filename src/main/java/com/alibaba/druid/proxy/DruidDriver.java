/**
 * Project: druid File Created at 2010-12-2 $Id$ Copyright 1999-2100 Alibaba.com Corporation Limited. All rights
 * reserved. This software is the confidential and proprietary information of Alibaba Company.
 * ("Confidential Information"). You shall not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into with Alibaba.com.
 */
package com.alibaba.druid.proxy;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import com.alibaba.druid.filter.Filter;
import com.alibaba.druid.proxy.config.AbstractDruidFilterConfig;
import com.alibaba.druid.proxy.config.DruidFilterConfigLoader;
import com.alibaba.druid.proxy.jdbc.DataSourceProxy;
import com.alibaba.druid.proxy.jdbc.DataSourceProxyConfig;
import com.alibaba.druid.proxy.jdbc.DataSourceProxyImpl;
import com.alibaba.druid.util.DruidLoaderUtils;
import com.alibaba.druid.util.JdbcUtils;

/**
 * @author shaojin.wensj
 */
public class DruidDriver implements Driver, DruidDriverMBean {

	private final static DruidDriver instance = new DruidDriver();

	private final static ConcurrentMap<String, DataSourceProxyImpl> dataSources = new ConcurrentHashMap<String, DataSourceProxyImpl>();
	private final static AtomicInteger dataSourceIdSeed = new AtomicInteger(0);

	public final static String DEFAULT_PREFIX = "jdbc:wrap-jdbc:";
	public final static String CONFIG_PREFIX = "druid.config";
	public final static String DRIVER_PREFIX = "driver=";
	public final static String PASSWORD_CALLBACK_PREFIX = "passwordCallback=";
	public final static String NAME_PREFIX = "name=";
	public final static String FILTERS_PREFIX = "filters=";

	private final AtomicLong connectCounter = new AtomicLong(0);

	private String acceptPrefix = DEFAULT_PREFIX;

	private int majorVersion = 4;

	private int minorVersion = 0;

	static {
		registerDriver(instance);
	}

	public static boolean registerDriver(Driver driver) {
		try {
			DriverManager.registerDriver(driver);

			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return false;
	}

	public DruidDriver() {

	}

	public static DruidDriver getInstance() {
		return instance;
	}

	public static int createDataSourceId() {
		return dataSourceIdSeed.incrementAndGet();
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		if (url == null) {
			return false;
		}

		if (url.startsWith(acceptPrefix)) {
			return true;
		}

		return false;
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		connectCounter.incrementAndGet();

		DataSourceProxyImpl dataSource = getDataSource(url, info);

		return dataSource.connect(info);
	}

	/**
	 * 参数定义： com.alibaba.druid.log.LogFilter=filter com.alibaba.druid.log.LogFilter.p1=prop-value com.alibaba.druid.log.LogFilter.p2=prop-value
	 * 
	 * @param url
	 * @return
	 * @throws SQLException
	 */
	private DataSourceProxyImpl getDataSource(String url, Properties info) throws SQLException {
		DataSourceProxyImpl dataSource = dataSources.get(url);

		if (dataSource == null) {
			String restUrl = url.substring(DEFAULT_PREFIX.length());

			DataSourceProxyConfig config;

			config = new DataSourceProxyConfig();

			List<AbstractDruidFilterConfig> druidFilterConfigList = new ArrayList<AbstractDruidFilterConfig>();
			String configFile = info.getProperty(CONFIG_PREFIX);
			if (configFile != null) {
				DruidFilterConfigLoader.loadConfig(configFile.trim(), druidFilterConfigList);
			}

			if (restUrl.startsWith(DRIVER_PREFIX)) {
				int pos = restUrl.indexOf(':', DRIVER_PREFIX.length());
				String driverText = restUrl.substring(DRIVER_PREFIX.length(), pos);
				if (driverText.length() > 0) {
					config.setRawDriverClassName(driverText.trim());
				}
				restUrl = restUrl.substring(pos + 1);
			}

			if (restUrl.startsWith(FILTERS_PREFIX)) {
				int pos = restUrl.indexOf(':', FILTERS_PREFIX.length());
				String filtersText = restUrl.substring(FILTERS_PREFIX.length(), pos);
				for (String filterItem : filtersText.split(",")) {
					DruidLoaderUtils.loadFilter(config.getFilters(), filterItem);
				}
				restUrl = restUrl.substring(pos + 1);
			}
			// 如果url中并无定义filter 采用配置
			if (config.getFilters().size() <= 0) {
				DruidLoaderUtils.loadFilter(config.getFilters(), druidFilterConfigList);
			}

			if (restUrl.startsWith(NAME_PREFIX)) {
				int pos = restUrl.indexOf(':', NAME_PREFIX.length());
				String name = restUrl.substring(NAME_PREFIX.length(), pos);
				config.setName(name);
				restUrl = restUrl.substring(pos + 1);
			}

			String rawUrl = restUrl;
			config.setRawUrl(rawUrl);

			if (config.getRawDriverClassName() == null) {
				String rawDriverClassname = JdbcUtils.getDriverClassName(rawUrl);
				config.setRawDriverClassName(rawDriverClassname);
			}

			config.setUrl(url);

			Driver rawDriver = createDriver(config.getRawDriverClassName());

			DataSourceProxyImpl newDataSource = new DataSourceProxyImpl(rawDriver, config);

			DataSourceProxy oldDataSource = dataSources.putIfAbsent(url, newDataSource); // 多线程处理需要
			if (oldDataSource == null) {
				// 放进去的线程负责初始化
				int dataSourceId = createDataSourceId();
				newDataSource.setId(dataSourceId);

				for (Filter filter : config.getFilters()) {
					filter.init(newDataSource);
				}
			}

			dataSource = dataSources.get(url);
		}
		return dataSource;
	}

	public Driver createDriver(String className) throws SQLException {
		Class<?> rawDriverClass = DruidLoaderUtils.loadClass(className);

		if (rawDriverClass == null) {
			throw new SQLException("jdbc-driver's class not found. '" + className + "'");
		}

		Driver rawDriver;
		try {
			rawDriver = (Driver) rawDriverClass.newInstance();
		} catch (InstantiationException e) {
			throw new SQLException("create driver instance error, driver className '" + className + "'");
		} catch (IllegalAccessException e) {
			throw new SQLException("create driver instance error, driver className '" + className + "'");
		}

		return rawDriver;
	}

	@Override
	public int getMajorVersion() {
		return this.majorVersion;
	}

	@Override
	public int getMinorVersion() {
		return this.minorVersion;
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		DataSourceProxyImpl dataSource = getDataSource(url, info);
		return dataSource.getRawDriver().getPropertyInfo(dataSource.getConfig().getRawUrl(), info);
	}

	@Override
	public boolean jdbcCompliant() {
		return true;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return null;
	}

	@Override
	public long getConnectCount() {
		return connectCounter.get();
	}

	public String getAcceptPrefix() {
		return acceptPrefix;
	}

	@Override
	public String[] getDataSourceUrls() {
		return dataSources.keySet().toArray(new String[dataSources.size()]);
	}

	public static ConcurrentMap<String, DataSourceProxyImpl> getDataSources() {
		return dataSources;
	}
}
