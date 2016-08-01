package com.pplive.pike.function.builtin;

import java.util.List;

import com.pplive.pike.expression.AbstractExpression;

public interface IBuiltinFunctionParser {

	AbstractExpression parse(String funcName, List<AbstractExpression> parameters);
}
