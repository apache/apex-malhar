/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 *
 */

/**
 * <b>{@link com.malhartech.lib.logs}</b> is a library operators for log line processing<p>
 * <br>
 * <br> The classes are<br>
 * <b>{@link com.malhartech.lib.logs.BaseLineTokenizer}</b>: Base class for line split operators. Takes in one stream via input port "data". Lines are split into tokens and tokens are processed<br>
 * <b>{@link com.malhartech.lib.logs.FilteredLineToTokenArrayList}</b>: Takes in one stream via input port "data". The tuples are String objects and are split into tokens. An ArrayList of all tokens that pass the filter are emitted on output port "tokens"<p>
 * <b>{@link com.malhartech.lib.logs.FilteredLineToTokenHashMap}</b>: Takes in one stream via input port "data". The tuples are String objects and are split into tokens. A HashMap of all filtered tokens are emitted on output port "tokens"<p>
 * <b>{@link com.malhartech.lib.logs.FilteredLineTokenizerKeyVal}</b>: Takes in one stream via input port "data". The tuples are String objects and are split into String tokens. Each token is emitted on output port "tokens" as key,val pair if the key exists in the filterby<p>
 * <b>{@link com.malhartech.lib.logs.LineToTokenArrayList}</b>: Takes in one stream via input port "data". The tuples are String objects and are split into tkns. An ArrayList of all tkns are emitted on output port "tokens". An ArrayList of all subtokens are emitted on port splittokens<p>
 * <b>{@link com.malhartech.lib.logs.LineToTokenHashMap}</b>: Takes in one stream via input port "data". The tuples are String objects and are split into tokens. A HashMap of all tokens are emitted on output port "tokens".<p>
 * <b>{@link com.malhartech.lib.logs.LineTokenizer}</b>: Takes in one stream via input port "data". The tuples are String objects and are split into String tokens. Each token is emitted on output port "tokens"<p>
 * <b>{@link com.malhartech.lib.logs.LineTokenizerKeyVal}</b>: Takes in one stream via input port "data". The tuples are String objects and are split into String tokens. Each token is emitted on output port "tokens" as a key,val pair<p>
 * <br>
 * <br>
 */

package com.malhartech.lib.logs;
