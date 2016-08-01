package com.pplive.pike.generator.trident;

import org.apache.commons.lang.builder.ToStringBuilder;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.impl.CaptureCollector;

//for PikeChainedAggregator
public class PikeChainedResult {
    Object[] objs;
    TridentCollector[] collectors;
    
    public PikeChainedResult(TridentCollector collector, int size) {
        objs = new Object[size];
        collectors = new TridentCollector[size];
        for(int i=0; i<size; i++) {
            if(size==1) {
                collectors[i] = collector;
            } else {
                collectors[i] = new CaptureCollector();                
            }
        }
    }
    
    public void setFollowThroughCollector(TridentCollector collector) {
        if(collectors.length>1) {
            for(TridentCollector c: collectors) {
                ((CaptureCollector) c).setCollector(collector);
            }
        }
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(objs);
    }    
}
