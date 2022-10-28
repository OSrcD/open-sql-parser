package com.alibaba.druid.sql.mysql.bvt;

import java.util.List;

import junit.framework.TestCase;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.druid.sql.parser.SQLStatementParser;

public class SQLSelectTest extends TestCase {
    public void test_select() throws Exception { // 已测试完
        String sql = "SELECT ALL FID FROM T1;SELECT DISTINCT FID FROM T1;SELECT DISTINCTROW FID FROM T1;";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        output(stmtList);
    }

    public void test_select_1() throws Exception {
        String sql =
                "SELECT HIGH_PRIORITY STRAIGHT_JOIN SQL_SMALL_RESULT SQL_BIG_RESULT SQL_BUFFER_RESULT FID FROM T1;";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        output(stmtList);
    }

    public void test_select_2() throws Exception {
        String sql = "SELECT SQL_CACHE FID FROM T1;";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        output(stmtList);
    }

    public void test_select_3() throws Exception {
        String sql = "SELECT SQL_NO_CACHE FID FROM T1;";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        output(stmtList);
    }

    public void test_select_4() throws Exception {
        String sql = "SELECT SQL_CALC_FOUND_ROWS FID FROM T1;";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        output(stmtList);
    }

    public void test_select_5() throws Exception {
        String sql = "SELECT 1 + 1;";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        output(stmtList);
    }

    public void test_select_6() throws Exception {
        String sql = "SELECT CONCAT(last_name,', ',first_name) AS full_name FROM mytable ORDER BY full_name;";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        output(stmtList);
    }

    private void output(List<SQLStatement> stmtList) {
        for (SQLStatement stmt : stmtList) {
            stmt.accept(new MySqlOutputVisitor(System.out)); // 访问 SQLSelectStatment
            System.out.println(";"); // 表语法的;结束
            System.out.println(); // 换行
        }
    }
}
