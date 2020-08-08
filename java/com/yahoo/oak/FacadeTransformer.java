package com.yahoo.oak;


import java.util.function.Function;

public interface FacadeTransformer<T> extends Function<NovaReadBuffer, T> {
}
