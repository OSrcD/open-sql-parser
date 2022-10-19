package com.alibaba.druid.mock;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class MockDriver implements Driver {
	private String prefix = "jdbc:fake:";
	private String mockPrefix = "jdbc:mock:";

	private final static MockDriver instance = new MockDriver();

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

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		MockConnection conn = new MockConnection(this);

		if (url == null) {
			return conn;
		}

		if (url.startsWith(prefix)) {
			String catalog = url.substring(prefix.length());
			conn.setCatalog(catalog);

			return conn;
		}
		
		if (url.startsWith(mockPrefix)) {
			String catalog = url.substring(mockPrefix.length());
			conn.setCatalog(catalog);
			
			return conn;
		}

		return null;
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		return url.startsWith(prefix);
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		return null;
	}

	@Override
	public int getMajorVersion() {
		return 0;
	}

	@Override
	public int getMinorVersion() {
		return 0;
	}

	@Override
	public boolean jdbcCompliant() {
		return true;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return null;
	}

	protected ResultSet createResultSet(MockStatement stmt, String sql) {
		MockResultSet rs = new MockResultSet(stmt);

		if ("SELECT 1".equalsIgnoreCase(sql)) {
			rs.getRows().add(new Object[] { 1 });
		} else if ("SELECT NOW()".equalsIgnoreCase(sql)) {
			rs.getRows().add(new Object[] { new java.sql.Timestamp(System.currentTimeMillis()) });
		} else if ("SELECT value FROM _int_1000_".equalsIgnoreCase(sql)) {
			for (int i = 0; i < 1000; ++i) {
				rs.getRows().add(new Object[] { i });
			}
		}

		return rs;
	}

	protected ResultSet createResultSet(MockPreparedStatement stmt) {
		MockResultSet rs = new MockResultSet(stmt);

		String sql = stmt.getSql();

		if ("SELECT 1".equalsIgnoreCase(sql)) {
			rs.getRows().add(new Object[] { 1 });
		} else if ("SELECT NOW()".equalsIgnoreCase(sql)) {
			rs.getRows().add(new Object[] { new java.sql.Timestamp(System.currentTimeMillis()) });
		} else if ("SELECT ?".equalsIgnoreCase(sql)) {
			rs.getRows().add(new Object[] { stmt.getParameters().get(0) });
		}

		return rs;
	}

	protected Clob createClob(MockConnection conn) throws SQLException {
		return new MockClob();
	}

	protected Blob createBlob(MockConnection conn) throws SQLException {
		return new MockBlob();
	}

	protected NClob createNClob(MockConnection conn) throws SQLException {
		return new MockNClob();
	}

	protected SQLXML createSQLXML(MockConnection conn) throws SQLException {
		return new MockSQLXML();
	}
}
