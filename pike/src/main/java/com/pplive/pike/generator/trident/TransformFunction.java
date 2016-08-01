package com.pplive.pike.generator.trident;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.pplive.pike.expression.AbstractExpression;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class TransformFunction extends BaseFunction {

	private static final long serialVersionUID = -8198792520720173149L;
	private List<AbstractExpression> exprList;

	public TransformFunction(List<AbstractExpression> exprList) {
		this.exprList = exprList;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf,
			TridentOperationContext context) {
		for(AbstractExpression ie : exprList){
			ie.init();
		}
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		List<Object> list = new ArrayList<Object>(exprList.size());
		for(AbstractExpression ie : exprList){
			list.add(ie.eval(tuple));
		}
		collector.emit(list);
	}

	@Override
	public void cleanup() {
	}

}
