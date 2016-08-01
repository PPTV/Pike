package com.pplive.examples.meta;

import com.pplive.pike.Configuration;
import com.pplive.pike.client.*;

/**
 * Created by jiatingjin on 2016/8/1.
 */
public class XmlMeta {

    PikeContextBuilder contextBuilder;

    void init() {
        Configuration config = new Configuration();
        config.put(Configuration.localMode, "true");
        config.put(Configuration.localRunSeconds, "20");
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        config.put(Configuration.SpoutLocalTextFile, classLoader.getResource("dol_smart").getPath());
        config.put(Configuration.SpoutGeneratorClass, "com.pplive.pike.generator.LocalTextFileSpoutGenerator");
        config.put(Configuration.MetaDataProviderClass, "com.pplive.pike.metadata.JsonMetaDataProvider");
        contextBuilder = new PikeContextBuilder(config);

        String topologyName = "sm_live_vv_5s";
        String sql = "withperiod 5s select channel, count(*) as vv from dol_smart group by channel";
        contextBuilder.withSql(sql).withTopologyName(topologyName);
    }

    public static void main(String[] args) {
        XmlMeta xmlMeta = new XmlMeta();
        xmlMeta.init();
        PikeContext context = xmlMeta.contextBuilder.build();
        context.submit();
    }

}
