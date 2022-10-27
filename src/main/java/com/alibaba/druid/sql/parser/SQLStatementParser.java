package com.alibaba.druid.sql.parser;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateViewStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLSetStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;

public class SQLStatementParser extends SQLParser {
    protected final SQLExprParser exprParser; // SQL 表达式解析器

    public SQLStatementParser(String sql) {
        super(sql);

        this.exprParser = createExprParser();
    }

    public SQLStatementParser(Lexer lexer) {
        super(lexer);
        this.exprParser = createExprParser();
    }

    protected SQLExprParser createExprParser() {
        return new SQLExprParser(lexer);
    }
    // 解析多条 SQL
    public List<SQLStatement> parseStatementList() throws ParserException {
        List<SQLStatement> statementList = new ArrayList<SQLStatement>();
        parseStatementList(statementList); // 重载方法解析
        return statementList;
    }

    public void parseStatementList(List<SQLStatement> statementList) throws ParserException {
        for (;;) {
            if (lexer.token() == Token.EOF) {
                return;
            }

            if (lexer.token() == (Token.SEMI)) { // 如果 token 为 ;
                lexer.nextToken();
                continue;
            }

            if (lexer.token() == (Token.SELECT)) {
                statementList.add(parseSelect()); // 解析 select 类型 调用子类方法
                continue;
            }

            if (lexer.token() == (Token.UPDATE)) {
                statementList.add(parseUpdateStatement());
                continue;
            }

            if (lexer.token() == (Token.CREATE)) {
                statementList.add(parseCreate());
                continue;
            }

            if (lexer.token() == (Token.INSERT)) {
                SQLInsertStatement insertStatement = parseInsert();
                statementList.add(insertStatement);

                continue;
            }

            if (lexer.token() == (Token.DELETE)) {
                statementList.add(parseDeleteStatement());
                continue;
            }

            if (lexer.token() == Token.SET) {
                statementList.add(parseSet());
                continue;
            }

            if (lexer.token() == Token.ALTER) {
                throw new ParserException("TODO");
            }

            if (lexer.token() == Token.DROP) {
                lexer.nextToken();

                if (lexer.token() == Token.TABLE) {
                    lexer.nextToken();
                    SQLName name = this.exprParser.name();
                    statementList.add(new SQLDropTableStatement(name));
                    continue;
                }
            }

            if (identifierEquals("CALL")) {
                SQLCallStatement stmt = parseCall();
                statementList.add(stmt);
                continue;
            }

            if (parseStatementListDialect(statementList)) {
                continue;
            }

            throw new ParserException("TODO " + lexer.token());
        }
    }

    public SQLInsertStatement parseInsert() {
        accept(Token.INSERT);
        accept(Token.INTO);

        SQLInsertStatement insertStatement = new SQLInsertStatement();

        SQLName tableName = this.exprParser.name();
        insertStatement.setTableName(tableName);

        if (lexer.token() == (Token.LPAREN)) {
            lexer.nextToken();
            this.exprParser.exprList(insertStatement.getColumns());
            accept(Token.RPAREN);
        }

        if (lexer.token() == (Token.VALUES)) {
            lexer.nextToken();
            accept(Token.LPAREN);
            SQLInsertStatement.ValuesClause values = new SQLInsertStatement.ValuesClause();
            this.exprParser.exprList(values.getValues());
            insertStatement.setValues(values);
            accept(Token.RPAREN);
        } else if (lexer.token() == (Token.SELECT)) {
            SQLQueryExpr queryExpr = (SQLQueryExpr) this.exprParser.expr();
            insertStatement.setQuery(queryExpr);
        }
        return insertStatement;
    }

    public boolean parseStatementListDialect(List<SQLStatement> statementList) {
        return false;
    }

    public SQLCallStatement parseCall() throws ParserException {
        acceptIdentifier("CALL");

        SQLCallStatement stmt = new SQLCallStatement();
        stmt.setProcedureName(exprParser.name());

        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();
            exprParser.exprList(stmt.getParameters());
            accept(Token.RPAREN);
        }

        return stmt;
    }

    public SQLSetStatement parseSet() throws ParserException {
        accept(Token.SET);
        SQLSetStatement stmt = new SQLSetStatement();

        for (;;) {
            SQLSetStatement.Item item = new SQLSetStatement.Item();
            item.setTarget(exprParser.primary());
            accept(Token.EQ);
            item.setValue(exprParser.expr());

            stmt.getItems().add(item);

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            } else {
                break;
            }
        }

        return stmt;
    }

    public SQLStatement parseCreate() throws ParserException {
        accept(Token.CREATE);

        Token token = lexer.token();

        if (token == Token.TABLE) {
            SQLCreateTableParser createTableParser = getSQLCreateTableParser();
            return createTableParser.parseCrateTable(false);
        }

        throw new ParserException("TODO " + lexer.token());
    }

    public SQLCreateTableParser getSQLCreateTableParser() {
        return new SQLCreateTableParser(lexer);
    }

    public SQLSelectStatement parseSelect() throws ParserException {
        return new SQLSelectStatement(createSQLSelectParser().select());
    }

    public SQLSelectParser createSQLSelectParser() {
        return new SQLSelectParser(this.lexer);
    }

    public SQLUpdateStatement parseUpdateStatement() throws ParserException {
        accept(Token.UPDATE);

        SQLUpdateStatement udpateStatement = new SQLUpdateStatement();

        udpateStatement.setTableName(this.exprParser.name());

        accept(Token.SET);

        for (;;) {
            SQLUpdateSetItem item = new SQLUpdateSetItem();
            item.setColumn(this.exprParser.name());
            accept(Token.EQ);
            item.setValue(this.exprParser.expr());

            udpateStatement.getItems().add(item);

            if (lexer.token() == (Token.COMMA)) {
                lexer.nextToken();
                continue;
            }

            break;
        }

        if (lexer.token() == (Token.WHERE)) {
            lexer.nextToken();
            udpateStatement.setWhere(this.exprParser.expr());
        }

        return udpateStatement;
    }

    public SQLDeleteStatement parseDeleteStatement() throws ParserException {
        lexer.nextToken();
        if (lexer.token() == (Token.FROM)) {
            lexer.nextToken();
        }

        SQLName tableName = exprParser.name();

        SQLDeleteStatement deleteStatement = new SQLDeleteStatement();
        deleteStatement.setTableName(tableName);

        if (lexer.token() == (Token.WHERE)) {
            lexer.nextToken();
            SQLExpr where = this.exprParser.expr();
            deleteStatement.setWhere(where);
        }

        return deleteStatement;
    }

    public SQLCreateTableStatement parseCreateTable() throws ParserException {
        // SQLCreateTableParser parser = new SQLCreateTableParser(this.lexer);
        // return parser.parseCrateTable();
        throw new ParserException("TODO");
    }

    public SQLCreateViewStatement parseCreateView() throws ParserException {
        SQLCreateViewStatement createView = new SQLCreateViewStatement();

        this.accept(Token.CREATE);

        this.accept(Token.VIEW);

        createView.setName(exprParser.name());

        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();
            this.exprParser.exprList(createView.getColumns());
            accept(Token.RPAREN);
        }

        this.accept(Token.AS);

        createView.setSubQuery(new SQLSelectParser(this.lexer).select());
        return createView;
    }
}
