package com.alibaba.druid.sql.dialect.oracle.ast.expr;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLExprImpl;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.dialect.oracle.ast.visitor.OracleASTVisitor;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

public class OracleGroupComparisonCondition extends SQLExprImpl {
    private final List<SQLExpr> exprList = new ArrayList<SQLExpr>();
    private SQLBinaryOperator operator;
    private OracleGroupComparator comparator;
    private final List<SQLExpr> targetExprList = new ArrayList<SQLExpr>();

    public OracleGroupComparisonCondition() {

    }

    public List<SQLExpr> getTargetExprList() {
        return this.targetExprList;
    }

    public SQLBinaryOperator getOperator() {
        return this.operator;
    }

    public void setOperator(SQLBinaryOperator operator) {
        this.operator = operator;
    }

    public OracleGroupComparator getComparator() {
        return this.comparator;
    }

    public void setComparator(OracleGroupComparator comparator) {
        this.comparator = comparator;
    }

    public List<SQLExpr> getExprList() {
        return this.exprList;
    }

    public void output(StringBuffer buf) {
        if (this.exprList.size() > 1) {
            buf.append("(");
        }
        for (int i = 0, size = this.exprList.size(); i < size; ++i) {
            if (i != 0) {
                buf.append(", ");
            }
            ((SQLExpr) this.exprList.get(i)).output(buf);
        }
        if (this.exprList.size() > 1) {
            buf.append(")");
        }

        buf.append(" ");
        buf.append(this.operator.name);
        buf.append(" ");
        buf.append(this.comparator.name());

        buf.append(" ");

        buf.append("(");
        for (int i = 0, size = this.targetExprList.size(); i < size; ++i) {
            if (i != 0) {
                buf.append(", ");
            }
            ((SQLExpr) this.targetExprList.get(i)).output(buf);
        }
        buf.append(")");
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        this.accept0((OracleASTVisitor) visitor);
    }

    protected void accept0(OracleASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, this.exprList);
            acceptChild(visitor, this.targetExprList);
        }

        visitor.endVisit(this);
    }

    public static enum OracleGroupComparator {
        ALL,
        ANY,
        SOME;
    }
}