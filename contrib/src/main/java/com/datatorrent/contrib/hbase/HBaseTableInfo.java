package com.datatorrent.contrib.hbase;

import com.datatorrent.contrib.common.TableInfo;

public class HBaseTableInfo extends TableInfo<HBaseFieldInfo>
{
	/**
	 * Siyuan is working on to send the real type to the app build. following methods are not required
	 * as App Builder don't support Generic, wrapper follow methods to show in App Builder.
	 */
	/**
	 * the field information. each field of the tuple related to on field info.
	 */
//	@Override
//	public List<HBaseFieldInfo> getFieldsInfo()
//	{
//		return super.getFieldsInfo();
//	}
//
//	/**
//	 * the field information. each field of the tuple related to on field info.
//	 */
//	@Override
//	public void setFieldsInfo(List<HBaseFieldInfo> fieldInfos)
//	{
//		super.setFieldsInfo(fieldInfos);
//	}
}
