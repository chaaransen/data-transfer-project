package org.datatransferproject.auth.icpsr;

import static org.datatransferproject.types.common.models.DataVertical.CALENDAR;
import static org.datatransferproject.types.common.models.DataVertical.NOTES;
import static org.datatransferproject.types.common.models.DataVertical.PHOTOS;
import static org.datatransferproject.types.common.models.DataVertical.SOCIAL_POSTS;
import static org.datatransferproject.types.common.models.DataVertical.VIDEOS;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.datatransferproject.auth.OAuth2Config;
import org.datatransferproject.types.common.models.DataVertical;

/**
 * Class that provides ICPSR auth information for OAuth2
 */
public class ICPSROAuthConfig implements OAuth2Config {

  public static final String DB_READ = "db.read";
  public static final String DB_WRITE = "db.write";

  @Override
  public String getServiceName() {
    return "ICPSR";
  }

  @Override
  public String getAuthUrl() {
    return "https://www.icpsr.com/auth";
  }

  @Override
  public String getTokenUrl() {
    return "https://auth.icpsr.com/api/auth";
  }

  @Override
  public Map<DataVertical, Set<String>> getExportScopes() {
    final Map<DataVertical, Set<String>> exportScopes = new HashMap<>();
    exportScopes.put(PHOTOS, ImmutableSet.of(DB_READ));
    exportScopes.put(VIDEOS, ImmutableSet.of(DB_READ));
    exportScopes.put(SOCIAL_POSTS, ImmutableSet.of(DB_READ));
    exportScopes.put(NOTES, ImmutableSet.of(DB_READ));
    exportScopes.put(CALENDAR, ImmutableSet.of(DB_READ));
    return Collections.unmodifiableMap(exportScopes);
  }

  @Override
  public Map<DataVertical, Set<String>> getImportScopes() {
    final Map<DataVertical, Set<String>> importScopes = new HashMap<>();
    importScopes.put(PHOTOS, ImmutableSet.of(DB_WRITE));
    importScopes.put(VIDEOS, ImmutableSet.of(DB_WRITE));
    importScopes.put(SOCIAL_POSTS, ImmutableSet.of(DB_WRITE));
    importScopes.put(NOTES, ImmutableSet.of(DB_WRITE));
    importScopes.put(CALENDAR, ImmutableSet.of(DB_WRITE));
    return Collections.unmodifiableMap(importScopes);
  }
}
