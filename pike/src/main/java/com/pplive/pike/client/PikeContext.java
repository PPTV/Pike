package com.pplive.pike.client;

import backtype.storm.generated.StormTopology;
import com.pplive.pike.Configuration;
import com.pplive.pike.PikeSqlCompiler;
import com.pplive.pike.PikeTopology;
import com.pplive.pike.TopologyConsoleDisplayer;

import com.pplive.pike.generator.ISpoutGenerator;
import com.pplive.pike.generator.trident.TridentTopologyGenerator;
import com.pplive.pike.metadata.MetaDataAdapter;
import com.pplive.pike.metadata.MetaDataProvider;
import com.pplive.pike.metadata.TableManager;
import com.pplive.pike.parser.LogicalQueryPlan;

import java.util.Map;


/**
 * Created by jiatingjin on 2016/7/26.
 */
public class PikeContext {


    private Configuration conf;

    private MetaDataProvider metaDataProvider;

    private String sql;

    private String topologyName;

    private ISpoutGenerator spoutGenerator;

    public PikeContext(Configuration conf, MetaDataProvider metaDataSource, String sql, String topologyName, ISpoutGenerator spoutGenerator){
        this.conf = conf;
        this.metaDataProvider = metaDataSource;
        this.sql = sql;
        this.topologyName = topologyName;
        this.spoutGenerator = spoutGenerator;
    }


    public void submit() {
        PikeTopology pikeTopology = compile();
        PikeSqlCompiler.EnsureTopologyMessageTimeout(conf, pikeTopology.period());

        if(conf.isLocalMode()) {
            PikeSqlCompiler.runLocally(topologyName, conf.localModeRunSeconds(), conf, pikeTopology.topology());
        } else {
            PikeSqlCompiler.submit(topologyName, conf, pikeTopology.topology());
        }
    }

    /**
     * 检查sql语法是否正确，无语义检查
     * @return
     */
    public boolean validateSQLSyntax() {
        return PikeSqlCompiler.parseSQLSyntax(sql);
    }

    public void explain() {
        boolean optimize = conf.isOptimizeQL();
        LogicalQueryPlan queryPlan = PikeSqlCompiler.parseSQL(sql,
                new TableManager(new MetaDataAdapter(metaDataProvider)), optimize);
        if (queryPlan != null) {
            System.out.println(queryPlan.toExplainString());
        }
    }

    public void display() {
        PikeTopology pikeTopology = compile();
        TopologyConsoleDisplayer.display(pikeTopology);
    }

    private PikeTopology compile(){
        Map<String, Object> options = PikeSqlCompiler.parseSQLOptions(sql);
        conf.putAll(options);
        LogicalQueryPlan queryPlan = PikeSqlCompiler.parseSQL(sql, new TableManager(new MetaDataAdapter(metaDataProvider)), conf.isOptimizeQL());
        boolean debug = conf.isDebugTopology();
        boolean localMode = conf.isLocalMode();
        if (conf.isDisplayTridentGraph()) {
            conf.put("$$tridentgraph", Boolean.TRUE);
        }

        if (conf.isValidForStorm() == false) {
            throw new RuntimeException(
                    "error: at least one option set in SQL or command line is not valid. Must be json-serializable");
        }

        TridentTopologyGenerator generator = new TridentTopologyGenerator();
        StormTopology stormTopology = generator.generate(topologyName,
                queryPlan, spoutGenerator, conf, debug, localMode);
        return new PikeTopology(queryPlan, stormTopology, conf);
    }


}
