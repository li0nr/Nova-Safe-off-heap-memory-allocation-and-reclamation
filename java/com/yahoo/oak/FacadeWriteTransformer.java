package com.yahoo.oak;

import java.util.function.Function;

public interface FacadeWriteTransformer<T> extends Function<NovaWriteBuffer, T> {
}
