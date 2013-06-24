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
 * limitations under the License. See accompanying LICENSE file.
 */
package com.datatorrent.contrib.kestrel;

import java.io.IOException;

public interface LineInputStream {
    
	/**
	 * Read everything up to the next end-of-line.  Does
	 * not include the end of line, though it is consumed
	 * from the input.
	 * @return  All next up to the next end of line.
	 */
	public String readLine() throws IOException;
	
	/**
	 * Read everything up to and including the end of line.
	 */
	public void clearEOL() throws IOException;
	
	/**
	 * Read some bytes.
	 * @param buf   The buffer into which read. 
	 * @return      The number of bytes actually read, or -1 if none could be read.
	 */
	public int read( byte[] buf ) throws IOException;
}
