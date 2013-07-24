/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.kestrel;

import java.io.*;

/**
 * Bridge class to provide nested Exceptions with IOException which has
 * constructors that don't take Throwables.
 * 
 * @version 1.2
 */
public class NestedIOException extends IOException {

    /**
     * Create a new <code>NestedIOException</code> instance.
     * @param cause object of type throwable
     */
    public NestedIOException( Throwable cause ) {
        super( cause.getMessage() );
        super.initCause( cause );
    }

    public NestedIOException( String message, Throwable cause ) {
        super( message );
        initCause( cause );
    }
}
