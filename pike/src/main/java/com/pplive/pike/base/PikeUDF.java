package com.pplive.pike.base;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.pplive.pike.exec.spoutproto.ColumnType;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface PikeUDF {

	String name();
	String description() default "";
	
	ColumnType valueType() default ColumnType.String;
	
	FunctionDeterminacy determinacy() default FunctionDeterminacy.Deterministic;
}
