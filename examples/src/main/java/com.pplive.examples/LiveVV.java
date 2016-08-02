package com.pplive.examples;

import com.pplive.pike.Configuration;
import com.pplive.pike.client.*;
import com.pplive.pike.exec.output.ConsoleOutput;
import com.pplive.examples.meta.provider.MyRawMetaDataProvider;

class LiveVV {


    PikeContextBuilder contextBuilder;

    void init() {
        Configuration config = new Configuration();
        config.put(Configuration.localMode, "true");
        config.put(Configuration.localRunSeconds, "20");
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        config.addResource(classLoader.getResource("examples.yaml"));

/*        SimpleMetaDataProvider dataSource = new SimpleMetaDataProvider();
        dataSource.addTable(t);*/

        config.put(Configuration.SpoutLocalTextFile, classLoader.getResource("dol_smart").getPath());
        config.put(Configuration.SpoutGeneratorClass, "com.pplive.pike.generator.LocalTextFileSpoutGenerator");

        contextBuilder = new PikeContextBuilder(config);
        contextBuilder.withMetaDataProvider(MyRawMetaDataProvider.class);

        contextBuilder.withOutput(ConsoleOutput.class, "local");

        String topologyName = "sm_live_vv_5s";
        String sql = "withperiod 5s select channel, count(*) as vv from dol_smart group by channel";
        contextBuilder.withSql(sql).withTopologyName(topologyName);
    }


    public void submit() {
        init();
        PikeContext context = contextBuilder.build();
        context.validateSQLSyntax();
        context.explain();
        context.display();
        context.submit();
    }

    public static void main(String[] args) {
        LiveVV liveVV = new LiveVV();
        liveVV.submit();
    }
}

