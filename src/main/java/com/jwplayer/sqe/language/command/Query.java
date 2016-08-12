package com.jwplayer.sqe.language.command;

import com.google.gson.GsonBuilder;
import com.jwplayer.sqe.language.clause.FromClause;
import com.jwplayer.sqe.language.clause.InsertIntoClause;
import com.jwplayer.sqe.language.clause.SelectClause;
import com.jwplayer.sqe.language.expression.BaseExpression;

public class Query extends BaseCommand {
    public InsertIntoClause insertInto;
    public SelectClause select;
    public FromClause from;
    public BaseExpression where;

    public Query(InsertIntoClause insertInto, SelectClause select, FromClause from, BaseExpression where) {
        this.insertInto = insertInto;
        this.select = select;
        this.from = from;
        this.where = where;
    }

    @Deprecated
    public static Query[] loadQueriesFromJson(String json) {
        GsonBuilder gson = new GsonBuilder();
        gson.registerTypeAdapter(BaseExpression.class, new BaseExpression.BaseExpressionTypeAdapter());
        return gson.create().fromJson(json, Query[].class);
    }

    @Override
    public CommandType getCommandType() {
        return CommandType.Query;
    }
}
