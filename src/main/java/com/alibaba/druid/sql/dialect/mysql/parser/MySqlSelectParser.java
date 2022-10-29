package com.alibaba.druid.sql.dialect.mysql.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLSetQuantifier;
import com.alibaba.druid.sql.ast.expr.SQLLiteralExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectGroupBy;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.parser.Lexer;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLSelectParser;
import com.alibaba.druid.sql.parser.Token;

public class MySqlSelectParser extends SQLSelectParser {

    public MySqlSelectParser(Lexer lexer) {
        super(lexer);
    }

    public MySqlSelectParser(String sql) throws ParserException {
        this(new MySqlLexer(sql));
        this.lexer.nextToken();
    }

    @Override
    protected SQLSelectQuery query() throws ParserException { // 重写了父类方法 解析
        if (lexer.token() == (Token.LPAREN)) { // 如果 Token 为 (
            lexer.nextToken();

            SQLSelectQuery select = query(); // 递归
            accept(Token.RPAREN);

            return queryRest(select); // all。union 重置
        }

        accept(Token.SELECT); // 用特定 token 访问获取下一个token
        // 返回上面的访问的 select token 再创一个 select 查询块对象
        MySqlSelectQueryBlock queryBlock = new MySqlSelectQueryBlock();

        if (lexer.token() == (Token.DISTINCT)) {
            queryBlock.setDistionOption(SQLSetQuantifier.DISTINCT); // 为 2 设置为去重
            lexer.nextToken();
        } else if (identifierEquals("DISTINCTROW")) {
            queryBlock.setDistionOption(SQLSetQuantifier.DISTINCTROW); // 标识为 2 DISTINCTROW 去重行
            lexer.nextToken();
        } else if (lexer.token() == (Token.ALL)) {
            queryBlock.setDistionOption(SQLSetQuantifier.ALL); // ALL 为 1 设置量词标记
            lexer.nextToken(); // 标记完 获取下一个 token
        }

        if (identifierEquals("HIGH_PRIORITY")) { // 优先执行操作
            queryBlock.setHignPriority(true);
            lexer.nextToken();
        }

        if (identifierEquals("STRAIGHT_JOIN")) {
            queryBlock.setStraightJoin(true);
            lexer.nextToken();
        }

        if (identifierEquals("SQL_SMALL_RESULT")) {
            queryBlock.setSmallResult(true);
            lexer.nextToken();
        }

        if (identifierEquals("SQL_BIG_RESULT")) {
            queryBlock.setBigResult(true);
            lexer.nextToken();
        }

        if (identifierEquals("SQL_BUFFER_RESULT")) {
            queryBlock.setBufferResult(true);
            lexer.nextToken();
        }

        if (identifierEquals("SQL_CACHE")) {
            queryBlock.setCache(true);
            lexer.nextToken();
        }

        if (identifierEquals("SQL_NO_CACHE")) {
            queryBlock.setCache(false);
            lexer.nextToken();
        }

        if (identifierEquals("SQL_CALC_FOUND_ROWS")) {
            queryBlock.setCalcFoundRows(true);
            lexer.nextToken();
        }

        parseSelectList(queryBlock); // 解析 select 中的列表

        if (lexer.token() == (Token.INTO)) {
            lexer.nextToken();
            acceptIdentifier("OUTFILE");
            SQLExpr outFile = expr();
            queryBlock.setOutFile(outFile);

            if (identifierEquals("FIELDS") || identifierEquals("COLUMNS")) {
                lexer.nextToken();

                if (identifierEquals("TERMINATED")) {
                    lexer.nextToken();
                    accept(Token.BY);
                }
                queryBlock.setOutFileColumnsTerminatedBy((SQLLiteralExpr) expr());

                if (identifierEquals("OPTIONALLY")) {
                    lexer.nextToken();
                    queryBlock.setOutFileColumnsEnclosedOptionally(true);
                }

                if (identifierEquals("ENCLOSED")) {
                    lexer.nextToken();
                    accept(Token.BY);
                    queryBlock.setOutFileColumnsEnclosedBy((SQLLiteralExpr) expr());
                }

                if (identifierEquals("ESCAPED")) {
                    lexer.nextToken();
                    accept(Token.BY);
                    queryBlock.setOutFileColumnsEscaped((SQLLiteralExpr) expr());
                }
            }

            if (identifierEquals("LINES")) {
                lexer.nextToken();

                if (identifierEquals("STARTING")) {
                    lexer.nextToken();
                    accept(Token.BY);
                    queryBlock.setOutFileLinesStartingBy((SQLLiteralExpr) expr());
                } else {
                    identifierEquals("TERMINATED");
                    lexer.nextToken();
                    accept(Token.BY);
                    queryBlock.setOutFileLinesTerminatedBy((SQLLiteralExpr) expr());
                }
            }
        }

        parseFrom(queryBlock); // 解析 from 语句

        parseWhere(queryBlock); // 解析 where 语句

        parseGroupBy(queryBlock); // 解析 group by 语句

        queryBlock.setOrderBy(this.createExprParser().parseOrderBy()); // 解析 order by 语句

        if (identifierEquals("LIMIT")) {
            lexer.nextToken();

            MySqlSelectQueryBlock.Limit limit = new MySqlSelectQueryBlock.Limit();

            SQLExpr temp = this.createExprParser().expr();
            if (lexer.token() == (Token.COMMA)) {
                limit.setOffset(temp);
                lexer.nextToken();
                limit.setRowCount(createExprParser().expr());
            } else if (identifierEquals("OFFSET")) {
                limit.setRowCount(temp);
                lexer.nextToken();
                limit.setOffset(createExprParser().expr());
            } else {
                limit.setRowCount(temp);
            }

            queryBlock.setLimit(limit);
        }

        if (identifierEquals("PROCEDURE")) {
            lexer.nextToken();
            throw new ParserException("TODO");
        }

        if (lexer.token() == Token.INTO) {
            lexer.nextToken();
            throw new ParserException("TODO");
        }

        if (lexer.token() == Token.FOR) {
            lexer.nextToken();
            accept(Token.UPDATE);

            queryBlock.setForUpdate(true);
        }

        if (lexer.token() == Token.LOCK) {
            lexer.nextToken();
            accept(Token.IN);
            acceptIdentifier("SHARE");
            acceptIdentifier("MODE");
            queryBlock.setLockInShareMode(true);
        }

        return queryRest(queryBlock); // 解析查询重置
    }

    protected void parseGroupBy(SQLSelectQueryBlock queryBlock) throws ParserException {
        if (lexer.token() == (Token.GROUP)) {
            lexer.nextToken();
            accept(Token.BY);

            SQLSelectGroupByClause groupBy = new SQLSelectGroupByClause();
            while (true) {
                groupBy.getItems().add(this.createExprParser().expr());
                if (!(lexer.token() == (Token.COMMA))) break;
                lexer.nextToken();
            }

            if (identifierEquals("WITH")) {
                lexer.nextToken();
                acceptIdentifier("ROLLUP");

                MySqlSelectGroupBy mySqlGroupBy = new MySqlSelectGroupBy();
                mySqlGroupBy.getItems().addAll(groupBy.getItems());
                mySqlGroupBy.setRollUp(true);

                groupBy = mySqlGroupBy;
            }

            if (lexer.token() == Token.HAVING) {
                lexer.nextToken();

                groupBy.setHaving(this.createExprParser().expr());
            }

            queryBlock.setGroupBy(groupBy);
        }
    }

    @Override
    protected MySqlExprParser createExprParser() { // 创建 MySQL 表达式解析器
        return new MySqlExprParser(lexer);
    }
}
