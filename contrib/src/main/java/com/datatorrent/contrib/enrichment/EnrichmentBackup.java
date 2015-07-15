package com.datatorrent.contrib.enrichment;

import com.datatorrent.lib.db.cache.CacheManager;
import java.util.List;

public interface EnrichmentBackup extends CacheManager.Backup
{
  public void setFields(List<String> lookupFields,List<String> includeFields);
  public boolean needRefresh();
}
