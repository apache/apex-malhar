/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.common.util.Slice;

/**
 * This codec is used for serializing the objects of class which implements
 * java.io.Serializable. It's not optimized for speed and should be used as the
 * last resort if you know that the slowness of it is not going to prevent you
 * from operating your application in realtime.
 *
 * @param <T>
 *          Type of the object which gets serialized/deserialized using this
 *          codec.
 * @since 0.3.3
 */
public class JavaSerializationStreamCodec<T extends Serializable> implements
		StreamCodec<T>
{
	@Override
	public Object fromByteArray(Slice fragment)
	{
		ByteArrayInputStream bis = new ByteArrayInputStream(fragment.buffer,
				fragment.offset, fragment.length);
		try {
			ObjectInputStream ois = new ObjectInputStream(bis);
			return ois.readObject();
		} catch (Exception ioe) {
			throw new RuntimeException(ioe);
		}
	}

	@Override
	public Slice toByteArray(T object)
	{
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try {
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(object);
			oos.flush();
			byte[] buffer = bos.toByteArray();
			return new Slice(buffer, 0, buffer.length);
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}

	@Override
	public int getPartition(T o)
	{
		return o.hashCode();
	}
}
