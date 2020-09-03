package com.yahoo.oak;


import java.util.function.Function;

public interface FacadeReadTransformer<T> extends Function<NovaReadBuffer, T> {
}
