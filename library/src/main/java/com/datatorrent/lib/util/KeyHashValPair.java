package com.datatorrent.lib.util;

import java.util.Map;

public class KeyHashValPair<K, V> extends KeyValPair<K, V> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7005592007894368002L;

	public KeyHashValPair(K k, V v) {
		super(k, v);
	}

	private KeyHashValPair() {
		super(null, null);
	}

	@Override
	public int hashCode() {
		return (getKey() == null ? 0 : getKey().hashCode());
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof Map.Entry))
			return false;
		Map.Entry e = (Map.Entry) o;
		return (this.getKey() == null ? e.getKey() == null : this.getKey().equals(e.getKey()));
	}

}
