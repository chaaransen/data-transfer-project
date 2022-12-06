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
    exportScopes.put(PHOTOS, ImmutableSet.of("db.read"));
    exportScopes.put(VIDEOS, ImmutableSet.of("db.read"));
    exportScopes.put(SOCIAL_POSTS, ImmutableSet.of("db.read"));
    exportScopes.put(NOTES, ImmutableSet.of("db.read"));
    exportScopes.put(CALENDAR, ImmutableSet.of("db.read"));
    return Collections.unmodifiableMap(exportScopes);
  }

  @Override
  public Map<DataVertical, Set<String>> getImportScopes() {
    final Map<DataVertical, Set<String>> importScopes = new HashMap<>();
    importScopes.put(PHOTOS, ImmutableSet.of("db.write"));
    importScopes.put(VIDEOS, ImmutableSet.of("db.write"));
    importScopes.put(SOCIAL_POSTS, ImmutableSet.of("db.write"));
    importScopes.put(NOTES, ImmutableSet.of("db.write"));
    importScopes.put(CALENDAR, ImmutableSet.of("db.write"));
    return Collections.unmodifiableMap(importScopes);
  }
}
