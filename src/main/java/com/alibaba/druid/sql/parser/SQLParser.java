package com.alibaba.druid.sql.parser;

public class SQLParser {
    protected final Lexer lexer; // 词法解析器

    public SQLParser(String sql) {
        this(new Lexer(sql));
        this.lexer.nextToken();
    }

    public SQLParser(Lexer lexer) {
        this.lexer = lexer;
    }

    protected boolean identifierEquals(String text) { // Token标识为 IDENTIFIER 相等 并且字符串相等
        return lexer.token() == Token.IDENTIFIER && lexer.stringVal().equalsIgnoreCase(text);
    }

    protected void acceptIdentifier(String text) {
        if (identifierEquals(text)) {
            lexer.nextToken();
        } else {
            setErrorEndPos(lexer.pos());
            throw new SQLParseException("syntax error, expect " + text + ", actual " + lexer.token());
        }
    }

    protected final String as() throws ParserException { // 列别名解析
        String rtnValue = null;

        if (lexer.token() == Token.AS) { // 如果为 AS 则解析
            lexer.nextToken();

            // QS_TODO remove alias token
            if (lexer.token() == Token.LITERAL_ALIAS) {
                rtnValue = "'" + lexer.stringVal() + "'";
                lexer.nextToken();
                return rtnValue;
            }

            if (lexer.token() == Token.IDENTIFIER) {
                rtnValue = lexer.stringVal();
                lexer.nextToken();
                return rtnValue;
            }

            throw new ParserException("Error", 0, 0);
        }

        if (lexer.token() == Token.LITERAL_ALIAS) { // 如果为 LITERAL_ALIAS 文字别名
            rtnValue = "'" + lexer.stringVal() + "'";
            lexer.nextToken();
        } else if (lexer.token() == Token.IDENTIFIER) { // 如果为标识符
            rtnValue = "'" + lexer.stringVal() + "'";
            lexer.nextToken();
        }
        return rtnValue;
    }

    public void accept(Token token) { // 相等扫描下一个字符 否则报语法错误
        if (lexer.token() == token) { // 当前 token 等于 传入进来 token
            lexer.nextToken();
        } else {
            setErrorEndPos(lexer.pos());
            throw new SQLParseException("syntax error, expect " + token + ", actual " + lexer.token());
        }
    }

    private int errorEndPos = -1; // 错误结束标志

    protected void setErrorEndPos(int errPos) {
        if (errPos > errorEndPos) errorEndPos = errPos;
    }
}
