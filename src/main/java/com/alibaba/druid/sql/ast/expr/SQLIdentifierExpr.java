package com.alibaba.druid.sql.ast.expr;

import com.alibaba.druid.sql.ast.SQLExprImpl;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;
// 标识符表达式
public class SQLIdentifierExpr extends SQLExprImpl implements SQLName {

    private static final long serialVersionUID = -4101240977289682659L;

    private String name;

    public SQLIdentifierExpr() {

    }

    public SQLIdentifierExpr(String name) {

        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void output(StringBuffer buf) {
        buf.append(this.name);
    }

    protected void accept0(SQLASTVisitor visitor) { // 打印标识符的 SQLIdentifierExpr.getName()
        visitor.visit(this);

        visitor.endVisit(this);
    }
}
