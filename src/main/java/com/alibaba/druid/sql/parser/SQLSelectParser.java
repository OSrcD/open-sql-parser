package com.alibaba.druid.sql.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLSetQuantifier;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUnionQuery;

public class SQLSelectParser extends SQLParser {

    public SQLSelectParser(String sql) {
        super(sql);
    }

    public SQLSelectParser(Lexer lexer) {
        super(lexer);
    }

    public SQLSelect select() throws ParserException { // 共同数据库厂商 解析 select 语句
        SQLSelect select = new SQLSelect();

        select.setQuery(query());
        select.setOrderBy(parseOrderBy()); // 外层的 order by

        if (select.getOrderBy() == null) {
            select.setOrderBy(parseOrderBy()); // 外层的 order by
        }

        return select;
    }
    // 解析查询重置
    public SQLSelectQuery queryRest(SQLSelectQuery selectQuery) throws ParserException {
        if (lexer.token() == Token.UNION) { // union
            lexer.nextToken();

            SQLUnionQuery union = new SQLUnionQuery();
            union.setLeft(selectQuery);

            if (lexer.token() == Token.ALL) { // all
                union.setAll(true);
                lexer.nextToken();
            }

            SQLSelectQuery right = this.query(); // 间接递归
            union.setRight(right);

            return union;
        }

        if (lexer.token() == Token.INTERSECT) { // 如果 token 为 INTERSECT 交集
            throw new ParserException("TODO");
        }

        if (lexer.token() == Token.MINUS) { // 如果 token 为 MINUS 减法
            throw new ParserException("TODO");
        }

        return selectQuery;
    }

    protected SQLSelectQuery query() throws ParserException {
        if (lexer.token() == Token.LPAREN) { // 子查询
            lexer.nextToken();

            SQLSelectQuery select = query(); // 递归
            accept(Token.RPAREN); // 右括号

            return queryRest(select); // union all 重置
        }

        accept(Token.SELECT);

        SQLSelectQueryBlock queryBlock = new SQLSelectQueryBlock();

        if (lexer.token() == Token.DISTINCT) {
            queryBlock.setDistionOption(SQLSetQuantifier.DISTINCT);
            lexer.nextToken();
        } else if (lexer.token() == Token.UNIQUE) {
            queryBlock.setDistionOption(SQLSetQuantifier.UNIQUE);
            lexer.nextToken();
        } else if (lexer.token() == Token.ALL) {
            queryBlock.setDistionOption(SQLSetQuantifier.ALL);
            lexer.nextToken();
        }

        parseSelectList(queryBlock); // 列查询 重置

        parseFrom(queryBlock); // from 解析 join left right重置

        parseWhere(queryBlock); // where 解析 重置

        parseGroupBy(queryBlock); // group 解析 重置

        return queryRest(queryBlock); // all，union 重置
    }

    protected void parseWhere(SQLSelectQueryBlock queryBlock) throws ParserException {
        if (lexer.token() == Token.WHERE) {
            lexer.nextToken();

            queryBlock.setWhere(expr());
        }
    }

    protected void parseGroupBy(SQLSelectQueryBlock queryBlock) throws ParserException {
        if (lexer.token() == Token.GROUP) {
            lexer.nextToken();
            accept(Token.BY);

            SQLSelectGroupByClause groupBy = new SQLSelectGroupByClause();
            while (true) {
                groupBy.getItems().add(expr());
                if (lexer.token() != Token.COMMA) {
                    break;
                }

                lexer.nextToken();
            }

            if (lexer.token() == Token.HAVING) {
                lexer.nextToken();

                groupBy.setHaving(expr());
            }

            queryBlock.setGroupBy(groupBy);
        }
    }

    protected void parseSelectList(SQLSelectQueryBlock queryBlock) throws ParserException {
        queryBlock.getSelectList().add(new SQLSelectItem(expr(), as()));

        while (lexer.token() == Token.COMMA) { // 如果为 , 继续解析 其实可以递归
            lexer.nextToken();
            queryBlock.getSelectList().add(new SQLSelectItem(expr(), as()));
        }
    }

    public void parseFrom(SQLSelectQueryBlock queryBlock) throws ParserException {
        if (lexer.token() != Token.FROM) {
            return;
        }

        lexer.nextToken();

        queryBlock.setFrom(parseTableSource());  // 解析表源
    }

    public SQLTableSource parseTableSource() throws ParserException { // 解析表源
        if (lexer.token() == Token.LPAREN) { // 如果 token 为 (
            lexer.nextToken();
            SQLTableSource tableSource;
            if (lexer.token() == Token.SELECT) {
                tableSource = new SQLSubqueryTableSource(select()); // 子查询解析 重新调用 select 方法
            } else if (lexer.token() == Token.LPAREN) {
                tableSource = parseTableSource(); // 递归
            } else {
                throw new ParserException("TODO");
            }

            accept(Token.RPAREN);

            return parseTableSourceRest(tableSource); // 多表关联查询 join 重置
        }

        if (lexer.token() == Token.SELECT) { // 如果 Token 为 SELECT
            throw new ParserException("TODO");
        }

        SQLExprTableSource tableReference = new SQLExprTableSource(); // from table 则为这种类型

        parseTableSourceQueryTableExpr(tableReference); // 解析查询表的表达式

        return parseTableSourceRest(tableReference); // 多表关联查询 join 重置
    }

    private void parseTableSourceQueryTableExpr(SQLExprTableSource tableReference) throws ParserException {
        tableReference.setExpr(expr()); // 设置表名表达式
    }

    private SQLTableSource parseTableSourceRest(SQLTableSource tableSource) throws ParserException {
        if ((tableSource.getAlias() == null) || (tableSource.getAlias().length() == 0)) { // 如果表别名为空 或者 表别名长度为 0
            if (lexer.token() != Token.LEFT && lexer.token() != Token.RIGHT && lexer.token() != Token.FULL) { // 如果 token 不为 LEFT 并且不为 RIGHT 并且不为 FULL
                tableSource.setAlias(as()); // 设置表别名 为空也会设置
            }
        }
        // 解析 join
        SQLJoinTableSource.JoinType joinType = null; // 内部静态枚举

        if (lexer.token() == Token.LEFT) { // 如果 token 为 LEFT
            lexer.nextToken();
            if (lexer.token() == Token.OUTER) {
                lexer.nextToken();
            }

            accept(Token.JOIN);
            joinType = SQLJoinTableSource.JoinType.LEFT_OUTER_JOIN;
        } else if (lexer.token() == Token.RIGHT) { // 如果 token 为 RIGHT
            lexer.nextToken();
            if (lexer.token() == Token.OUTER) {
                lexer.nextToken();
            }
            accept(Token.JOIN);
            joinType = SQLJoinTableSource.JoinType.RIGHT_OUTER_JOIN;
        } else if (lexer.token() == Token.FULL) {
            lexer.nextToken();
            if (lexer.token() == Token.OUTER) {
                lexer.nextToken();
            }
            accept(Token.JOIN);
            joinType = SQLJoinTableSource.JoinType.FULL_OUTER_JOIN;
        } else if (lexer.token() == Token.INNER) {
            lexer.nextToken();
            accept(Token.JOIN);
            joinType = SQLJoinTableSource.JoinType.INNER_JOIN;
        } else if (lexer.token() == Token.JOIN) {
            lexer.nextToken();
            joinType = SQLJoinTableSource.JoinType.JOIN;
        } else if (lexer.token() == Token.COMMA) { // 如果 token 为 ,
            lexer.nextToken();
            joinType = SQLJoinTableSource.JoinType.COMMA;
        }

        if (joinType != null) { // 如果表链接为不空
            SQLJoinTableSource join = new SQLJoinTableSource();
            join.setLeft(tableSource);
            join.setJoinType(joinType);
            join.setRight(parseTableSource()); // 间接递归

            if (lexer.token() == Token.ON) {
                lexer.nextToken();
                join.setCondition(expr());
            }

//            return join;
            return parseTableSourceRest(join); // 递归
        }

        return tableSource;
    }

    public SQLExpr expr() { // 解析表达式
        return createExprParser().expr(); // 创建解析表达式并解析
    }

    protected SQLExprParser createExprParser() {
        return new SQLExprParser(lexer);
    }

    public SQLOrderBy parseOrderBy() throws ParserException {
        return createExprParser().parseOrderBy();
    }

    public void acceptKeyword(String ident) {
        if (lexer.token() == Token.IDENTIFIER && ident.equalsIgnoreCase(lexer.stringVal())) {
            lexer.nextToken();
        } else {
            setErrorEndPos(lexer.pos());
            throw new SQLParseException("syntax error, expect " + ident + ", actual " + lexer.token());
        }
    }

}
