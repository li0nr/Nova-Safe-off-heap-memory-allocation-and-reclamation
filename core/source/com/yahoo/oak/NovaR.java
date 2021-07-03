/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak;

import java.util.function.Function;

/**
 * An interface to be supported by keys and values provided for Oak's mapping
 */
public interface NovaR<T> extends Function<Long,T> {}

