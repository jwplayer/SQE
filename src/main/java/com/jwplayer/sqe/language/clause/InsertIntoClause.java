package com.jwplayer.sqe.language.clause;

import org.apache.storm.trident.state.StateType;

import java.util.List;
import java.util.Map;


public class InsertIntoClause {
    public String objectName;
    public String stateName;
    public StateType stateType;
    public List<String> fields;
    public Map options;

    public InsertIntoClause(String objectName, String stateName, StateType stateType, List<String> fields, Map options) {
        this.objectName = objectName;
        this.stateName = stateName;
        this.stateType = stateType;
        this.fields = fields;
        this.options = options;
    }
}