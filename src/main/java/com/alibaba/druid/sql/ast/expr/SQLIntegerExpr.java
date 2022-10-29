package com.alibaba.druid.sql.ast.expr;

import com.alibaba.druid.sql.visitor.SQLASTVisitor;
// 数字类型的表达式 比如 1
public class SQLIntegerExpr extends SQLNumericLiteralExpr {
    private static final long serialVersionUID = 1L;

    private Number number;

    public SQLIntegerExpr(Number number) {

        this.number = number;
    }

    public SQLIntegerExpr() {

    }

    public Number getNumber() {
        return this.number;
    }

    public void setNumber(Number number) {
        this.number = number;
    }

    public void output(StringBuffer buf) {
        buf.append(this.number);
    }

    protected void accept0(SQLASTVisitor visitor) {
        visitor.visit(this);

        visitor.endVisit(this);
    }
}
