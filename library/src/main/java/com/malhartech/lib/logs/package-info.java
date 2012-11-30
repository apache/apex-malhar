/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 *
 */

/**
 * <b>{@link com.malhartech.lib.logs}</b> is a library operators for log line processing<p>
 * <br>
 * <br> The operators are<br>
 * <b>{@link com.malhartech.lib.logs.FilteredLineToTokenArrayList}</b>: Takes in one stream via input port "data". The tuples are String objects and are split into tokens. An ArrayList of all tokens that pass the filter are emitted on output port "tokens"<br>
 * <b>{@link com.malhartech.lib.logs.FilteredLineToTokenHashMap}</b>: Takes in one stream via input port "data". The tuples are String objects and are split into tokens. A HashMap of all filtered tokens are emitted on output port "tokens"<br>
 * <b>{@link com.malhartech.lib.logs.FilteredLineTokenizerKeyVal}</b>: Takes in one stream via input port "data". The tuples are String objects and are split into String tokens. Each token is emitted on output port "tokens" as key,val pair if the key exists in the filterby<br>
 * <b>{@link com.malhartech.lib.logs.LineToTokenArrayList}</b>: Takes in one stream via input port "data". The tuples are String objects and are split into tkns. An ArrayList of all tkns are emitted on output port "tokens". An ArrayList of all subtokens are emitted on port splittokens<br>
 * <b>{@link com.malhartech.lib.logs.LineToTokenHashMap}</b>: Takes in one stream via input port "data". The tuples are String objects and are split into tokens. A HashMap of all tokens are emitted on output port "tokens".<br>
 * <b>{@link com.malhartech.lib.logs.LineTokenizer}</b>: Takes in one stream via input port "data". The tuples are String objects and are split into String tokens. Each token is emitted on output port "tokens"<br>
 * <b>{@link com.malhartech.lib.logs.LineTokenizerKeyVal}</b>: Takes in one stream via input port "data". The tuples are String objects and are split into String tokens. Each token is emitted on output port "tokens" as a key,val pair<br>
 * <br>
 * <br>
 */

package com.malhartech.lib.logs;
