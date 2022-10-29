package com.alibaba.druid.sql.dialect.mysql.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLHexExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlBinaryExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlCharExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlExtractExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlIntervalExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlIntervalUnit;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlMatchAgainstExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlMatchAgainstExpr.SearchModifier;
import com.alibaba.druid.sql.parser.Lexer;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLExprParser;
import com.alibaba.druid.sql.parser.SQLSelectParser;
import com.alibaba.druid.sql.parser.Token;

public class MySqlExprParser extends SQLExprParser {
    public MySqlExprParser(Lexer lexer) {
        super(lexer);
    }

    public MySqlExprParser(String sql) throws ParserException {
        this(new MySqlLexer(sql));
        this.lexer.nextToken();
    }
    // 如果为 标识符 并且为 REGEXP 则重置
    public SQLExpr relationalRest(SQLExpr expr) throws ParserException {
        if (lexer.token() == Token.IDENTIFIER && "REGEXP".equalsIgnoreCase(lexer.stringVal())) {
            lexer.nextToken();
            SQLExpr rightExp = bitOr();

            rightExp = relationalRest(rightExp);

            return new SQLBinaryOpExpr(expr, SQLBinaryOperator.RegExp, rightExp);
        }

        return super.relationalRest(expr); // 调用父类重置
    }
    // Token 为IDENTIFIER 并且内容为 MOD 取余 需要重置
    public SQLExpr multiplicativeRest(SQLExpr expr) throws ParserException {
        if (lexer.token() == Token.IDENTIFIER && "MOD".equalsIgnoreCase(lexer.stringVal())) {
            lexer.nextToken();
            SQLExpr rightExp = bitOr();

            rightExp = relationalRest(rightExp);

            return new SQLBinaryOpExpr(expr, SQLBinaryOperator.Modulus, rightExp);
        }

        return super.multiplicativeRest(expr); // 父类重置
    }

    public SQLExpr notRationalRest(SQLExpr expr) {
        if (lexer.token() == Token.IDENTIFIER && "REGEXP".equalsIgnoreCase(lexer.stringVal())) {
            lexer.nextToken();
            SQLExpr rightExp = bitOr();

            rightExp = relationalRest(rightExp);

            return new SQLBinaryOpExpr(expr, SQLBinaryOperator.NotRegExp, rightExp);
        }

        return super.notRationalRest(expr);
    }

    public final SQLExpr primaryRest(SQLExpr expr) throws ParserException {
        if (expr == null) {
            throw new IllegalArgumentException("expr");
        }

        if (lexer.token() == Token.LITERAL_CHARS) { // 文字字符需要重置
            if (expr instanceof SQLIdentifierExpr) {
                SQLIdentifierExpr identExpr = (SQLIdentifierExpr) expr;
                String ident = identExpr.getName();

                if (ident.equalsIgnoreCase("x")) {
                    String charValue = lexer.stringVal();
                    lexer.nextToken();
                    expr = new SQLHexExpr(charValue);

                    return primaryRest(expr);
                } else if (ident.equalsIgnoreCase("b")) {
                    String charValue = lexer.stringVal();
                    lexer.nextToken();
                    expr = new MySqlBinaryExpr(charValue);

                    return primaryRest(expr);
                } else if (ident.startsWith("_")) {
                    String charValue = lexer.stringVal();
                    lexer.nextToken();

                    MySqlCharExpr mysqlCharExpr = new MySqlCharExpr(charValue);
                    mysqlCharExpr.setCharset(identExpr.getName());
                    if (identifierEquals("COLLATE")) {
                        lexer.nextToken();

                        String collate = lexer.stringVal();
                        mysqlCharExpr.setCollate(collate);
                        accept(Token.IDENTIFIER);
                    }

                    expr = mysqlCharExpr;

                    return primaryRest(expr);
                } else if (ident.equalsIgnoreCase("BINARY")) {
                    String charValue = lexer.stringVal();
                    lexer.nextToken();

                    MySqlCharExpr mysqlCharExpr = new MySqlCharExpr(charValue);
                    mysqlCharExpr.setCharset("BINARY");
                    expr = mysqlCharExpr;

                    return primaryRest(expr);
                }
            } else if (expr instanceof SQLCharExpr) {
                SQLMethodInvokeExpr concat = new SQLMethodInvokeExpr("CONCAT");
                concat.getParameters().add(expr);
                do {
                    String chars = lexer.stringVal();
                    concat.getParameters().add(new SQLCharExpr(chars));
                    lexer.nextToken();
                } while (lexer.token() == Token.LITERAL_CHARS);
                expr = concat;
            }
        } else if (lexer.token() == Token.IDENTIFIER) { // 标识符token 需要重置
            if (expr instanceof SQLHexExpr) {
                if ("USING".equalsIgnoreCase(lexer.stringVal())) {
                    lexer.nextToken();
                    if (lexer.token() != Token.IDENTIFIER) {
                        throw new ParserException("syntax error, illegal hex");
                    }
                    String charSet = lexer.stringVal();
                    lexer.nextToken();
                    expr.getAttributes().put("USING", charSet);

                    return primaryRest(expr);
                }
            } else if ("COLLATE".equalsIgnoreCase(lexer.stringVal())) {
                lexer.nextToken();

                if (lexer.token() != Token.IDENTIFIER) {
                    throw new ParserException("syntax error");
                }

                String collate = lexer.stringVal();
                lexer.nextToken();

                SQLBinaryOpExpr binaryExpr =
                        new SQLBinaryOpExpr(expr, SQLBinaryOperator.COLLATE, new SQLIdentifierExpr(collate));

                expr = binaryExpr;

                return primaryRest(expr);
            } else if (expr instanceof SQLVariantRefExpr) {
                if ("COLLATE".equalsIgnoreCase(lexer.stringVal())) {
                    lexer.nextToken();

                    if (lexer.token() != Token.IDENTIFIER) {
                        throw new ParserException("syntax error");
                    }

                    String collate = lexer.stringVal();
                    lexer.nextToken();

                    expr.putAttribute("COLLATE", collate);

                    return primaryRest(expr);
                }
            } else if (expr instanceof SQLIntegerExpr) {
                SQLIntegerExpr intExpr = (SQLIntegerExpr) expr;
                String binaryString = lexer.stringVal();
                if (intExpr.getNumber().intValue() == 0 && binaryString.startsWith("b")) {
                    lexer.nextToken();
                    expr = new MySqlBinaryExpr(binaryString.substring(1));

                    return primaryRest(expr);
                }
            }
        }
        // 下一个 token 为 (  需要重置 并且当前表达为 标识符表达SQL
        if (lexer.token() == Token.LPAREN && expr instanceof SQLIdentifierExpr) {
            SQLIdentifierExpr identExpr = (SQLIdentifierExpr) expr;
            String ident = identExpr.getName();

            if ("EXTRACT".equalsIgnoreCase(ident)) {
                lexer.nextToken();
                SQLMethodInvokeExpr methodInvokeExpr = new SQLMethodInvokeExpr(ident);

                if (lexer.token() != Token.IDENTIFIER) {
                    throw new ParserException("syntax error");
                }

                String unitVal = lexer.stringVal();
                MySqlIntervalUnit unit = MySqlIntervalUnit.valueOf(unitVal);
                lexer.nextToken();

                accept(Token.FROM);

                SQLExpr value = expr();

                MySqlExtractExpr extract = new MySqlExtractExpr();
                extract.setValue(value);
                extract.setUnit(unit);
                accept(Token.RPAREN);

                expr = extract;

                return primaryRest(expr);
            } else if ("SUBSTRING".equalsIgnoreCase(ident)) {
                lexer.nextToken();
                SQLMethodInvokeExpr methodInvokeExpr = new SQLMethodInvokeExpr(ident);
                for (;;) {
                    SQLExpr param = expr();
                    methodInvokeExpr.getParameters().add(param);

                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    } else if (lexer.token() == Token.FROM) {
                        lexer.nextToken();
                        SQLExpr from = expr();
                        methodInvokeExpr.putAttribute("FROM", from);

                        if (lexer.token() == Token.FOR) {
                            lexer.nextToken();
                            SQLExpr _for = expr();
                            methodInvokeExpr.putAttribute("FOR", _for);
                        }
                        break;
                    } else if (lexer.token() == Token.RPAREN) {
                        break;
                    } else {
                        throw new ParserException("syntax error");
                    }
                }

                accept(Token.RPAREN);
                expr = methodInvokeExpr;

                return primaryRest(expr);
            } else if ("TRIM".equalsIgnoreCase(ident)) {
                lexer.nextToken();
                SQLMethodInvokeExpr methodInvokeExpr = new SQLMethodInvokeExpr(ident);

                if (lexer.token() == Token.IDENTIFIER) {
                    String flagVal = lexer.stringVal();
                    if ("LEADING".equalsIgnoreCase(flagVal)) {
                        lexer.nextToken();
                        methodInvokeExpr.getAttributes().put("TRIM_TYPE", "LEADING");
                    } else if ("BOTH".equalsIgnoreCase(flagVal)) {
                        lexer.nextToken();
                        methodInvokeExpr.getAttributes().put("TRIM_TYPE", "BOTH");
                    } else if ("TRAILING".equalsIgnoreCase(flagVal)) {
                        lexer.nextToken();
                        methodInvokeExpr.putAttribute("TRIM_TYPE", "TRAILING");
                    }
                }

                SQLExpr param = expr();
                methodInvokeExpr.getParameters().add(param);

                if (lexer.token() == Token.FROM) {
                    lexer.nextToken();
                    SQLExpr from = expr();
                    methodInvokeExpr.putAttribute("FROM", from);
                }

                accept(Token.RPAREN);
                expr = methodInvokeExpr;

                return primaryRest(expr);
            } else if ("MATCH".equalsIgnoreCase(ident)) {
                lexer.nextToken();
                MySqlMatchAgainstExpr matchAgainstExpr = new MySqlMatchAgainstExpr();

                if (lexer.token() == Token.RPAREN) {
                    lexer.nextToken();
                } else {
                    exprList(matchAgainstExpr.getColumns());
                    accept(Token.RPAREN);
                }

                acceptIdentifier("AGAINST");

                accept(Token.LPAREN);
                SQLExpr against = primary();
                matchAgainstExpr.setAgainst(against);

                if (lexer.token() == Token.IN) {
                    lexer.nextToken();
                    if (identifierEquals("NATURAL")) {
                        lexer.nextToken();
                        acceptIdentifier("LANGUAGE");
                        acceptIdentifier("MODE");
                        if (identifierEquals("WITH")) {
                            lexer.nextToken();
                            acceptIdentifier("QUERY");
                            acceptIdentifier("EXPANSION");
                            matchAgainstExpr.setSearchModifier(SearchModifier.IN_NATURAL_LANGUAGE_MODE_WITH_QUERY_EXPANSION);
                        } else {
                            matchAgainstExpr.setSearchModifier(SearchModifier.IN_NATURAL_LANGUAGE_MODE);
                        }
                    } else {
                        throw new ParserException("TODO");
                    }
                } else if (identifierEquals("WITH")) {
                    throw new ParserException("TODO");
                }

                accept(Token.RPAREN);

                expr = matchAgainstExpr;

                return primaryRest(expr);
            } else if ("CONVERT".equalsIgnoreCase(ident)) {
                lexer.nextToken();
                SQLMethodInvokeExpr methodInvokeExpr = new SQLMethodInvokeExpr(ident);

                if (lexer.token() != Token.RPAREN) {
                    exprList(methodInvokeExpr.getParameters());
                }

                if (identifierEquals("USING")) {
                    lexer.nextToken();
                    if (lexer.token() != Token.IDENTIFIER) {
                        throw new ParserException("syntax error");
                    }
                    String charset = lexer.stringVal();
                    lexer.nextToken();
                    methodInvokeExpr.putAttribute("USING", charset);
                }

                accept(Token.RPAREN);

                expr = methodInvokeExpr;

                return primaryRest(expr);
            }
        }

        return super.primaryRest(expr); // 调用父类进行重置
    }

    protected SQLSelectParser createSelectParser() {
        return new MySqlSelectParser(this.lexer);
    }

    protected SQLExpr parseInterval() {
        accept(Token.INTERVAL);

        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();

            SQLMethodInvokeExpr methodInvokeExpr = new SQLMethodInvokeExpr("INTERVAL");
            if (lexer.token() != Token.RPAREN) {
                exprList(methodInvokeExpr.getParameters());
            }

            accept(Token.RPAREN);

            return primaryRest(methodInvokeExpr);
        } else {
            SQLExpr value = expr();

            if (lexer.token() != Token.IDENTIFIER) {
                throw new ParserException("Syntax error");
            }

            String unit = lexer.stringVal();
            lexer.nextToken();

            MySqlIntervalExpr intervalExpr = new MySqlIntervalExpr();
            intervalExpr.setValue(value);
            intervalExpr.setUnit(MySqlIntervalUnit.valueOf(unit));

            return intervalExpr;
        }
    }
}
