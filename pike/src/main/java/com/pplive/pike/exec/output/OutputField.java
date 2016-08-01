package com.pplive.pike.exec.output;

import java.io.Serializable;
import java.util.List;

import com.pplive.pike.exec.spoutproto.ColumnType;
import com.pplive.pike.expression.ConstantExpression;
import storm.trident.tuple.TridentTuple;

import com.pplive.pike.base.Immutable;
import com.pplive.pike.expression.AbstractExpression;
import com.pplive.pike.util.TupleUtil;

@Immutable
public final class OutputField implements Serializable{

	private static final long serialVersionUID = 4223516834706193490L;
	private final String name;
	private final ColumnType valueType;
	private final int index;

	private final AbstractExpression _expr; // ColumnExpression, ConstantExpression or FunctionExpression with no arguments

    private final List<ConstantExpression> _outputContexts;
    public boolean hasOutputContext() {
        return this._outputContexts != null && this._outputContexts.size() > 0;
    }

	private int _cachedIndex = -1;
	
	public OutputField(String name, ColumnType type){
		this(name, type, -1);
	}
	
	public OutputField(String name, AbstractExpression expr){
		this(name, getOutputType(expr), -1, expr);
	}
	
	public OutputField(String name, ColumnType type, int index){
		this(name, type, index, null);
	}
	
	private OutputField(String name, ColumnType type, int index,  AbstractExpression expr){
		this.name = name;
		this.valueType = type;
		this.index = index;
		this._cachedIndex = index;
		
		this._expr = expr;
        if (expr != null) {
            this._outputContexts = ExpressionOutputContextGetter.getAllOutputContextRefs(expr);
        }
        else {
            this._outputContexts = null;
        }
	}
	
	private static ColumnType getOutputType(AbstractExpression expr){
		return ColumnType.convert(expr.exprType());
	}
	
	public String getName() {
		return name;
	}

	public ColumnType getValueType() {
		return valueType;
	}

	public int getIndex() {
		return index;
	}
	
	public void init(){
		if (this._expr != null){
			this._expr.init();
		}
	}

    public Object eval(TridentTuple tuple, OutputContext context) {
        if (this._outputContexts != null) {
            for(ConstantExpression expr : this._outputContexts) {
                assert expr.exprType() == OutputContext.class;
                assert expr.eval(null) == null;
                expr.setValue(context);
            }
        }
        Object value = eval(tuple);
        if (this._outputContexts != null) {
            for(ConstantExpression expr : this._outputContexts) {
                expr.setValue(null);
            }
        }
        return value;
    }

    public Object eval(TridentTuple tuple) {
		if (this._expr != null){
			return this._expr.eval(tuple);
		}
		
		if (this._cachedIndex >= 0) {
			assert this._cachedIndex < tuple.size();
			if (this._cachedIndex < tuple.size()){
				return tuple.getValue(this._cachedIndex);
			}
			// todo: need log warning, if come here something must be wrong 
		}
		
		this._cachedIndex = TupleUtil.findIndex(tuple, this.name);
		if (this._cachedIndex < 0){
			return null;
		}
		
		Object o = tuple.getValue(this._cachedIndex);
		return o;
	}
}
