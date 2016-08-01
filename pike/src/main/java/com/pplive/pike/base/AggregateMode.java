package com.pplive.pike.base;

public enum AggregateMode implements java.io.Serializable {
	Regular,  // when statistics period is same as output period
	Moving,   // moving time window
	Accumulating  // accumulate in a statistics period, clean/reset when next statistics period begin
}
