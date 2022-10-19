/**
 * Project: druid
 * 
 * File Created at 2010-12-2
 * $Id$
 * 
 * Copyright 1999-2100 Alibaba.com Corporation Limited.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * Alibaba Company. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Alibaba.com.
 */
package com.alibaba.druid.proxy.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import com.alibaba.druid.filter.FilterChain;
import com.alibaba.druid.filter.FilterChainImpl;

/**
 * 
 * @author shaojin.wensj
 *
 */
public class ConnectionProxyImpl extends WrapperProxyImpl implements ConnectionProxy {
	private final Connection connection;

	private final DataSourceProxy dataSource;
	
	private final Properties properties;
	
	private final long connectedTime;

	public ConnectionProxyImpl(DataSourceProxy dataSource, Connection connection, Properties properties, long id) {
		super(connection, id);
		this.dataSource = dataSource;
		this.connection = connection;
		this.properties = properties;
		this.connectedTime = System.currentTimeMillis();
	}
	
	public Date getConnectedTime() {
		return new Date(connectedTime);
	}

	public Properties getProperties() {
		return properties;
	}

	public Connection getConnectionRaw() {
		return connection;
	}

	public DataSourceProxy getDirectDataSource() {
		return this.dataSource;
	}

	public FilterChain createChain() {
		return new FilterChainImpl(dataSource);
	}

	@Override
	public void clearWarnings() throws SQLException {
		createChain().connection_clearWarnings(this);
	}

	@Override
	public void close() throws SQLException {
		createChain().connection_close(this);
	}

	@Override
	public void commit() throws SQLException {
		createChain().connection_commit(this);
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		return createChain().connection_createArrayOf(this, typeName, elements);
	}

	@Override
	public Blob createBlob() throws SQLException {
		return createChain().connection_createBlob(this);
	}

	@Override
	public Clob createClob() throws SQLException {
		return createChain().connection_createClob(this);
	}

	@Override
	public NClob createNClob() throws SQLException {
		return createChain().connection_createNClob(this);
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		return createChain().connection_createSQLXML(this);
	}

	@Override
	public Statement createStatement() throws SQLException {
		return createChain().connection_createStatement(this);
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		return createChain().connection_createStatement(this, resultSetType, resultSetType);
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		return createChain().connection_createStatement(this, resultSetType, resultSetType, resultSetHoldability);
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		return createChain().connection_createStruct(this, typeName, attributes);
	}

	@Override
	public void setSchema(String schema) throws SQLException {

	}

	@Override
	public String getSchema() throws SQLException {
		return null;
	}

	@Override
	public void abort(Executor executor) throws SQLException {

	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {

	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		return 0;
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		return createChain().connection_getAutoCommit(this);
	}

	@Override
	public String getCatalog() throws SQLException {
		return createChain().connection_getCatalog(this);
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		return createChain().connection_getClientInfo(this);
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		return createChain().connection_getClientInfo(this, name);
	}

	@Override
	public int getHoldability() throws SQLException {
		return createChain().connection_getHoldability(this);
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		return createChain().connection_getMetaData(this);
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		return createChain().connection_getTransactionIsolation(this);
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return createChain().connection_getTypeMap(this);
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return createChain().connection_getWarnings(this);
	}

	@Override
	public boolean isClosed() throws SQLException {
		return createChain().connection_isClosed(this);
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return createChain().connection_isReadOnly(this);
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		return createChain().connection_isValid(this, timeout);
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		return createChain().connection_nativeSQL(this, sql);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		return createChain().connection_prepareCall(this, sql);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		return createChain().connection_prepareCall(this, sql, resultSetType, resultSetConcurrency);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		return createChain().connection_prepareCall(this, sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return createChain().connection_prepareStatement(this, sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		return createChain().connection_prepareStatement(this, sql, autoGeneratedKeys);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		return createChain().connection_prepareStatement(this, sql, columnIndexes);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		return createChain().connection_prepareStatement(this, sql, columnNames);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		return createChain().connection_prepareStatement(this, sql, resultSetType, resultSetConcurrency);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		return createChain().connection_prepareStatement(this, sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		createChain().connection_releaseSavepoint(this, savepoint);
	}

	@Override
	public void rollback() throws SQLException {
		createChain().connection_rollback(this);
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		createChain().connection_rollback(this, savepoint);
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		createChain().connection_setAutoCommit(this, autoCommit);
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		createChain().connection_setCatalog(this, catalog);
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		createChain().connection_setClientInfo(this, properties);
	}

	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		createChain().connection_setClientInfo(this, name, value);
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		createChain().connection_setHoldability(this, holdability);
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		createChain().connection_setReadOnly(this, readOnly);
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		return createChain().connection_setSavepoint(this);
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		return createChain().connection_setSavepoint(this, name);
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		createChain().connection_setTransactionIsolation(this, level);
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		createChain().connection_setTypeMap(this, map);
	}

}
