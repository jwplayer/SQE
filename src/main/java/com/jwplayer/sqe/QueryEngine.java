package com.jwplayer.sqe;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.jwplayer.sqe.language.clause.InsertIntoClause;
import com.jwplayer.sqe.language.command.BaseCommand;
import com.jwplayer.sqe.language.command.CommandType;
import com.jwplayer.sqe.language.command.CreateStream;
import com.jwplayer.sqe.language.command.Query;
import com.jwplayer.sqe.language.command.SetCommand;
import com.jwplayer.sqe.language.expression.BaseExpression;
import com.jwplayer.sqe.language.expression.ConstantExpression;
import com.jwplayer.sqe.language.expression.ExpressionType;
import com.jwplayer.sqe.language.expression.FieldExpression;
import com.jwplayer.sqe.language.expression.FunctionExpression;
import com.jwplayer.sqe.language.expression.FunctionType;
import com.jwplayer.sqe.language.expression.aggregation.AggregationExpression;
import com.jwplayer.sqe.language.expression.transform.TransformExpression;
import com.jwplayer.sqe.language.serde.BaseDeserializer;
import com.jwplayer.sqe.language.state.StateAdapter;
import com.jwplayer.sqe.language.state.StateOperationType;
import com.jwplayer.sqe.language.stream.InputStreamType;
import com.jwplayer.sqe.language.stream.StreamAdapter;
import com.jwplayer.sqe.language.stream.StreamContainer;
import com.jwplayer.sqe.trident.ValueFilter;
import com.jwplayer.sqe.trident.function.AddField;
import com.jwplayer.sqe.trident.function.GetTupleFromAvro;
import com.jwplayer.sqe.trident.function.MapField;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.fluent.ChainedFullAggregatorDeclarer;
import org.apache.storm.trident.fluent.ChainedPartitionAggregatorDeclarer;
import org.apache.storm.trident.fluent.GroupedStream;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.tuple.Fields;


public class QueryEngine {
    private Map<String, BaseCommand> commands = new LinkedHashMap<>();
    private Map<String, StreamContainer> inputStreams = new HashMap<>();
    private Map optionMap;
    private String topologyName;

    public QueryEngine(String topologyName, Map optionMap) {
        this.optionMap = optionMap;
        this.topologyName = topologyName;
    }

    public static class Options implements Serializable {
        public int parallelismHint = 1;

        public static Options parse(Map map) {
            Options options = new Options();

            if(map.containsKey("jw.sqe.parallelism.hint")) options.parallelismHint = (int) map.get("jw.sqe.parallelism.hint");

            return options;
        }
    }

    // TODO: This is for backwards compatibility, remove later
    public Map<String, Stream> build() {
        return build(null);
    }

    /*
        This builds the necessary components (each, aggregate, etc) onto the input streams to compute the queries.
        While I'm trying to keep this flexible for future expansion, there are some pretty big assumptions in play:

        - TODO: If more than one query is supplied, the query engine will create a big parent query that is the
          union of all of the fields and aggregations of the child queries. This *should* be a huge performance
          boost for the first few sets of queries we want to do, but the query engine is not smart here and can be
          greatly improved in the future. The parent query will also be generated in a brute force way: the query engine
          won't try to determine if it can combine or choose a single transform to meet multiple transforms downstream.
          All transforms are promoted to the parent and no longer appear in the children queries. Aggregations will be
          run in both the parent and child queries.
     */
    @SuppressWarnings("unchecked")
    public Map<String, Stream> build(TridentTopology topology) {
       Options options = Options.parse(optionMap);
        if(commands.size() == 0) throw new RuntimeException("There are no registered commands");
       
        // Determine needed fields
        
        Map<String, Set<String>> requiredFieldsPerStreamMap = new HashMap<>();
       
        for(Map.Entry<String, BaseCommand> command: commands.entrySet()) {
            // Only check Query commands
            if(command.getValue().getCommandType() == CommandType.Query) {
                Query query = (Query) command.getValue();
                Set<String> requiredFieldsForQuery = new HashSet<>();
                requiredFieldsForQuery.add(StreamAdapter.STREAM_METADATA_FIELD);
                requiredFieldsForQuery.addAll(getFields(query.select.expressions));

                if (query.where != null)
                    requiredFieldsForQuery.addAll(getFields(query.where));
                
                if (requiredFieldsPerStreamMap.containsKey(query.from.objectName)){
                    Set<String> FieldsFromMapForStream = requiredFieldsPerStreamMap.get(query.from.objectName);
                    requiredFieldsForQuery.addAll(FieldsFromMapForStream);
                }
                                  
                requiredFieldsPerStreamMap.put(query.from.objectName, requiredFieldsForQuery);                
            }
        }

        // Process non-query commands
        for(Map.Entry<String, BaseCommand> command: commands.entrySet()) {
            switch(command.getValue().getCommandType()) {
                case CreateStream:
                    if(topology == null) {
                        throw new RuntimeException("The query engine cannot run CreateStream commands if topology is null");
                    }
                    CreateStream createStream = (CreateStream) command.getValue();
                    Map streamOptions = new HashMap();
                    streamOptions.putAll(this.optionMap);
                    if(createStream.options != null) streamOptions.putAll(createStream.options);
                    StreamAdapter streamAdapter = StreamAdapter.makeAdapter(createStream.spoutName, streamOptions);
                    Stream cStream = streamAdapter.makeStream(
                            topology,
                            topologyName,
                            createStream.streamName,
                            createStream.objectName,
                            createStream.spoutType
                    );
                    cStream = cStream.parallelismHint(options.parallelismHint);
                    if(createStream.deserializer != null) {
                        BaseDeserializer deserializer = BaseDeserializer.makeDeserializer(createStream.deserializer, streamOptions);
                        Set<String> requiredFieldsForInputStream = requiredFieldsPerStreamMap.get(createStream.streamName);
                        List<String> deFields = new ArrayList<>(requiredFieldsForInputStream);
                        // TODO: Is it better to handle metadata fields in a special way?
                        // For example, track them separately, and handle them specially as needed
                        for(String field: cStream.getOutputFields()) {
                            if(deFields.contains(field)) {
                                deFields.remove(field);
                            }
                        }
                        cStream = deserializer.deserialize(cStream, new Fields(deFields));
                        cStream = cStream.project(new Fields(new ArrayList<>(requiredFieldsForInputStream)));
                    }
                    registerInputStream(createStream.streamName, cStream);
                case Query:
                    // We don't process queries at this point
                    break;
                case Set:
                    SetCommand setCommand = (SetCommand) command.getValue();
                    optionMap.put(setCommand.key, setCommand.value);
                    break;
            }
        }

        if(inputStreams.size() == 0) throw new RuntimeException("There are no registered streams. No queries can be run.");

        // Process input streams according to stream type
        // TODO: This is deprecated. Handling of deserializing of the stream should be handled by the
        // create stream command or by the code that creates the stream
        Map<String, Stream> tupleStreams = new HashMap<>();
        for(Map.Entry<String, StreamContainer> inputStream: inputStreams.entrySet()) {
            String InputStreamName = inputStream.getValue().streamName;
            Set<String> requiredFieldForInputStream = requiredFieldsPerStreamMap.get(InputStreamName);
            switch(inputStream.getValue().inputStreamType) {
                case BinaryAvro:
                    tupleStreams.put(inputStream.getKey(),
                            getTupleFromAvro(inputStream.getKey(), inputStream.getValue().stream,
                                    new Fields(new ArrayList<>(requiredFieldForInputStream))));
                    break;
                case Tuple:
                    tupleStreams.put(inputStream.getKey(),
                            inputStream.getValue().stream.project(new Fields(new ArrayList<>(requiredFieldForInputStream))));
                    break;
                default:
                    throw new RuntimeException(inputStream.getValue().inputStreamType +
                            " is not a supported InputStreamType");
            }
        }

        // Build the appropriate components for each query
        // TODO: No query plan is built, no optimization is done (like a parent query)
        for(Map.Entry<String, BaseCommand> command: commands.entrySet()) {
            switch(command.getValue().getCommandType()) {
                case Query:
                    Query query = (Query) command.getValue();
                    Stream queryStream = tupleStreams.get(query.from.objectName);
                    Set<BaseExpression> keys = getKeys(query.select.expressions);
                    if(query.where != null) queryStream = processFilters(queryStream, query.where);
                    queryStream = processTransforms(queryStream, query.select.expressions);
                    queryStream = queryStream.name("_T");
                    if(hasAggregateExpressions(query.select.expressions)) {
                        queryStream = processAggregators(queryStream, keys, query.select.expressions);
                        queryStream = queryStream.name(command.getKey() + "_A");
                    }
                    InsertIntoClause insert = query.insertInto;

                    if (insert.stateName != null && !insert.stateName.equals("")) {
                        persistState(queryStream, insert, query.select.expressions);
                    } 
                    queryStream = mapOutputFields(queryStream, query.select.expressions, insert.fields);
                    tupleStreams.put(query.insertInto.objectName, queryStream);
                    
                    break;
            }
        }

        return tupleStreams;
    }

    private Set<AggregationExpression> getAggregators(List<BaseExpression> expressions) {
        Set<AggregationExpression> aggregators = new HashSet<>();

        for(BaseExpression expression: expressions) {
            switch(expression.getExpressionType()) {
                case Function:
                    if(((FunctionExpression) expression).getFunctionType() == FunctionType.Aggregation)
                        aggregators.add((AggregationExpression) expression);
                    break;
            }
        }

        return aggregators;
    }

    private Set<String> getFields(BaseExpression expression) {
        Set<String> fields = new HashSet<>();

        switch(expression.getExpressionType()) {
            case Field:
                fields.add(((FieldExpression) expression).fieldName);
                break;
            case Function:
                fields.addAll(getFields(((FunctionExpression) expression).getArguments()));
                break;
        }

        return fields;
    }

    private Set<String> getFields(List<BaseExpression> expressions) {
        Set<String> fields = new HashSet<>();

        for(BaseExpression expression: expressions) {
            fields.addAll(getFields(expression));
        }

        return fields;
    }

    private Set<BaseExpression> getKeys(List<BaseExpression> expressions) {
        Set<BaseExpression> keys = new HashSet<>();

        for(BaseExpression expression: expressions) {
            switch(expression.getExpressionType()) {
                case Constant:
                    keys.add(expression);
                    break;
                case Field:
                    keys.add(expression);
                    break;
                case Function:
                    if(((FunctionExpression) expression).getFunctionType() == FunctionType.Transform)
                        keys.add(expression);
                    break;
            }
        }

        return keys;
    }

    private Stream getTupleFromAvro(String schemaName, Stream inputStream, Fields outputFields) {
        return inputStream
                .each(new Fields("bytes"), new GetTupleFromAvro(schemaName, outputFields), outputFields)
                .project(outputFields);
    }

    private Boolean hasAggregateExpressions(List<BaseExpression> expressions) {
        Boolean retVal = false;

        for(BaseExpression expression: expressions) {
            if(expression.getExpressionType() == ExpressionType.Function
                    && ((FunctionExpression) expression).getFunctionType() == FunctionType.Aggregation) {
                retVal = true;
            }
        }

        return retVal;
    }

    private Stream mapOutputFields(Stream queryStream, List<BaseExpression> queryExpressions,
                                   List<String> outputFields) {
        List<String> queryFields = new ArrayList<>();
        if(queryStream.getOutputFields().contains(StreamAdapter.STREAM_METADATA_FIELD)) {
            queryFields.add(StreamAdapter.STREAM_METADATA_FIELD);
            for(BaseExpression expression: queryExpressions) queryFields.add(expression.getOutputFieldName());

            queryStream = queryStream.project(new Fields(queryFields));
            for(int i = 1; i < queryFields.size(); i++) {
                if(!queryStream.getOutputFields().contains(outputFields.get(i - 1))) {
                    queryStream = queryStream.each(new Fields(queryFields.get(i)), new MapField(), new Fields(outputFields.get(i - 1)));
                }
            }
        } else {
            for(BaseExpression expression: queryExpressions) queryFields.add(expression.getOutputFieldName());

            queryStream = queryStream.project(new Fields(queryFields));
            for(int i = 0; i < queryFields.size(); i++) {
                if(!queryStream.getOutputFields().contains(outputFields.get(i))) {
                    queryStream = queryStream.each(new Fields(queryFields.get(i)), new MapField(), new Fields(outputFields.get(i)));
                }
            }
        }

        List<String> ofClone = new ArrayList<>();
        if(queryStream.getOutputFields().contains(StreamAdapter.STREAM_METADATA_FIELD)) ofClone.add(StreamAdapter.STREAM_METADATA_FIELD);
        ofClone.addAll(outputFields);
        queryStream = queryStream.project(new Fields(ofClone));

        return queryStream;
    }

    @SuppressWarnings("unchecked")
    private void persistState(Stream queryStream, InsertIntoClause insert, List<BaseExpression> expressions) {
        List<String> keyFieldNames = new ArrayList<>();
        List<String> insertKeys = new ArrayList<>();

        for(int i = 0; i < expressions.size(); i++) {
            if(expressions.get(i).getExpressionType() != ExpressionType.Function
                    || ((FunctionExpression) expressions.get(i)).getFunctionType() != FunctionType.Aggregation) {
                insertKeys.add(insert.fields.get(i));
                keyFieldNames.add(expressions.get(i).getOutputFieldName());
            }
        }

        Fields keyFields = new Fields(keyFieldNames);

        Map stateOptions = new HashMap();
        stateOptions.putAll(this.optionMap);
        if(insert.options != null) stateOptions.putAll(insert.options);
        StateAdapter stateAdapter = StateAdapter.makeAdapter(insert.stateName.toLowerCase(), stateOptions);

        if(hasAggregateExpressions(expressions)) {
            GroupedStream gStream = queryStream.groupBy(keyFields);
            for (int i = 0; i < expressions.size(); i++) {
                if (expressions.get(i).getExpressionType() == ExpressionType.Function
                        && ((FunctionExpression) expressions.get(i)).getFunctionType() == FunctionType.Aggregation) {
                    AggregationExpression aExpression = (AggregationExpression) expressions.get(i);
                    StateFactory aggStateFactory =
                            stateAdapter.makeFactory(
                                    insert.objectName,
                                    insertKeys,
                                    insert.fields.get(i),
                                    insert.stateType, StateOperationType.AGGREGATE);
                    aExpression.persistentAggregate(gStream, new Fields(expressions.get(i).getOutputFieldName()),
                            aggStateFactory, new Fields(insert.fields.get(i)));
                }
            }
        } else {
            StateFactory stateFactory = stateAdapter.makeFactory(insert.objectName, insertKeys, null, insert.stateType, StateOperationType.NONAGGREGATE);
            stateAdapter.partitionPersist(queryStream, stateFactory, keyFields);
        }
    }

    private Stream processAggregators(Stream queryStream, Set<BaseExpression> keys, List<BaseExpression> expressions) {
        Set<AggregationExpression> aggregators = getAggregators(expressions);
        List<String> keyFieldNames = new ArrayList<>();

        for(BaseExpression expression: keys) {
            keyFieldNames.add(expression.getOutputFieldName());
        }

        Fields keyFields = new Fields(keyFieldNames);
        ChainedPartitionAggregatorDeclarer pChain = queryStream.groupBy(keyFields).chainedAgg();

        for(AggregationExpression expression: aggregators) {
            String inputField = ((FieldExpression) expression.getArguments().get(0)).fieldName;
            pChain = expression.partitionAggregate(pChain, new Fields(inputField),
                    new Fields("p_" + expression.getOutputFieldName()));
        }

        ChainedFullAggregatorDeclarer fChain = pChain.chainEnd().groupBy(keyFields).chainedAgg();

        for(AggregationExpression expression: aggregators) {
            fChain = expression.aggregate(fChain, new Fields("p_" + expression.getOutputFieldName()),
                    new Fields(expression.getOutputFieldName()));
        }

        return fChain.chainEnd();
    }

    private Stream processFilters(Stream queryStream, BaseExpression expression) {
        List<BaseExpression> expressions = new ArrayList<>();

        expressions.add(expression);
        queryStream = processTransforms(queryStream, expressions);

        switch (expression.getExpressionType()) {
            case Constant:
                Object constant = ((ConstantExpression) expression).constant;
                if(constant instanceof Boolean) {
                    return queryStream.each(new Fields(expression.getOutputFieldName()), new ValueFilter());
                } else {
                    throw new RuntimeException("A constant expression can only be used as the top level expression in a WHERE clause if it is Boolean");
                }
            case Field:
                throw new RuntimeException("A field expression is not valid in a WHERE clause");
            case Function:
                return queryStream.each(new Fields(expression.getOutputFieldName()), new ValueFilter());
            default:
                throw new RuntimeException(expression.getExpressionType() + " is not a valid expression type");
        }
    }

    private Stream processTransforms(Stream queryStream, List<BaseExpression> expressions) {
        List<BaseExpression> unRolledExpressions = new ArrayList<>();

        for(BaseExpression expression: expressions) {
            List<BaseExpression> temp = expression.unRoll();
            if(temp != null) unRolledExpressions.addAll(temp);
            unRolledExpressions.add(expression);
        }

        for(BaseExpression expression: unRolledExpressions) {
            switch (expression.getExpressionType()) {
                case Constant:
                    ConstantExpression cExp = (ConstantExpression) expression;
                    if(!queryStream.getOutputFields().contains(cExp.getOutputFieldName())) {
                        queryStream = queryStream.each(
                                new Fields(),
                                new AddField(cExp.constant),
                                new Fields(cExp.getOutputFieldName()));
                    }
                    break;
                case Field:
                    // Do nothing, the field is already in the stream
                    break;
                case Function:
                    if (((FunctionExpression) expression).getFunctionType() == FunctionType.Transform &&
                            !queryStream.getOutputFields().contains(expression.getOutputFieldName())) {
                        TransformExpression tExp = (TransformExpression) expression;
                        queryStream = tExp.transform(queryStream);
                        break;
                    }
            }
        }

        return queryStream;
    }

    public void registerInputStream(String streamName, Stream stream) {
        inputStreams.put(streamName, new StreamContainer(InputStreamType.Tuple, stream, streamName));
    }

    public void registerCommands(Iterable<BaseCommand> commands) {
        for(BaseCommand command: commands) {
            // TODO: Maaaybe I need a better way of handling the command name
            switch(command.getCommandType()) {
                case CreateStream:
                    CreateStream createStream = (CreateStream) command;
                    registerCommand(createStream.streamName, createStream);
                    break;
                case Query:
                    Query query = (Query) command;
                    registerCommand(query.insertInto.objectName, query);
                    break;
                case Set:
                    SetCommand setCommand = (SetCommand) command;
                    registerCommand(setCommand.key, command);
                    break;
            }
        }
    }

    public void registerCommand(String commandName, BaseCommand command) {
        commands.put(commandName, command);
    }
}
