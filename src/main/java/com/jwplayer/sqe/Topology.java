package com.jwplayer.sqe;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.storm.starter.util.StormRunner;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;

import com.jwplayer.sqe.language.command.BaseCommand;
import com.jwplayer.sqe.util.FileHelper;
import com.jwplayer.sqe.util.YamlConfig;


public class Topology {
    List<?> commandPaths;
    Map<String, Object> config = new HashMap<>();
    List<?> configPaths;
    QueryEngine queryEngine;
    Boolean runLocally;
    Integer runtime;
    Config topologyConfig = new Config();
    String topologyName;
   
    

    public StormTopology createTopology() {
        TridentTopology topology = new TridentTopology();

        queryEngine.build(topology);

        return topology.build();
    }

    public OptionParser getOptionParser() {
        return new OptionParser() {
            {
                accepts("config",
                        "Path(s) to one or more YAML config files that is added to the Storm or SQE options")
                        .withRequiredArg();
                accepts("commands",
                        "Path(s) to one or more JSON files containing SQE commands. Commands are executed in order.")
                        .withRequiredArg();
                accepts("local", "If set, runs the topology locally");
                accepts("name", "The name of the topology").withRequiredArg();
                accepts("runtime",
                        "If running locally, this determines how long the topology runs (in seconds)")
                        .withRequiredArg().defaultsTo("120");
                accepts("help").forHelp();
            }
        };
    }

    public void loadCommands() throws IOException, URISyntaxException {
        if (commandPaths != null) {
            for (Object commandPath : commandPaths) {
                URI uri = new URI((String) commandPath);
                String json = FileHelper.loadFileAsString(uri);
                queryEngine.registerCommands(BaseCommand.load(json));
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void loadConfig() throws IOException, URISyntaxException {
        for(Object configPath: configPaths) {
            Map<String, Object> tempConfig;
            if (configPath != null) {
                tempConfig = YamlConfig.getJWConfigAsMap((String) configPath);
            } else {
                tempConfig = new HashMap<>();
            }

            if (tempConfig.containsKey("Storm")) {
                topologyConfig.putAll((Map) tempConfig.get("Storm"));
            }
            if (tempConfig.containsKey("SQE")) {
                config.putAll((Map) tempConfig.get("SQE"));
            }
        }
        
       
    }

    public void setCommandLineOptions(OptionSet options) {
        configPaths = options.valuesOf("config");
        commandPaths = options.valuesOf("commands");
        runLocally = options.has("local");
        runtime = Integer.parseInt((String) options.valueOf("runtime"));
        topologyName = (String) options.valueOf("name");
    }

    public void run(String args[]) throws Exception {
        OptionParser parser = getOptionParser();
        OptionSet options = parser.parse(args);

        if (options.has("help")) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        setCommandLineOptions(options);
        loadConfig();
        queryEngine = new QueryEngine(topologyName, config);
        loadCommands();

        if (runLocally) {
            StormRunner.runTopologyLocally(createTopology(), topologyName, topologyConfig, runtime);
        } else {
            StormSubmitter.submitTopology(topologyName, topologyConfig,createTopology());
        }
    }

    public static void main(String args[]) throws Exception {
        Topology topology = new Topology();
        topology.run(args);
    }
}
