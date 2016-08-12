package com.jwplayer.sqe.language.clause;

import com.jwplayer.sqe.language.expression.BaseExpression;

import java.util.List;

public class SelectClause {
    public List<BaseExpression> expressions;

    public SelectClause(List<BaseExpression> expressions) {
        this.expressions = expressions;
    }
}
