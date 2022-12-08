/*
 * Copyright 2022 The Data Transfer Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.datatransferproject.transfer.icpsr.videos;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import org.datatransferproject.api.launcher.Monitor;
import org.datatransferproject.spi.cloud.connection.ConnectionProvider;
import org.datatransferproject.spi.cloud.storage.TemporaryPerJobDataStore;
import org.datatransferproject.spi.transfer.idempotentexecutor.IdempotentImportExecutor;
import org.datatransferproject.spi.transfer.idempotentexecutor.ItemImportResult;
import org.datatransferproject.spi.transfer.provider.ImportResult;
import org.datatransferproject.spi.transfer.provider.Importer;
import org.datatransferproject.transfer.icpsr.common.ICPSRDataTransferClient;
import org.datatransferproject.transfer.icpsr.common.ICPSRDataTransferClientFactory;
import org.datatransferproject.types.common.models.videos.VideoModel;
import org.datatransferproject.types.common.models.videos.VideosContainerResource;
import org.datatransferproject.types.transfer.auth.TokenSecretAuthData;

public class ICPSRVideosImporter
    implements Importer<TokenSecretAuthData, VideosContainerResource> {

  private static final String VIDEO_TRANSFER_MAIN_FOLDER = "Video Transfer";

  private final TemporaryPerJobDataStore jobStore;
  private final ConnectionProvider connectionProvider;
  private final Monitor monitor;
  private final ICPSRDataTransferClientFactory clientFactory;

  public ICPSRVideosImporter(
      Monitor monitor,
      TemporaryPerJobDataStore jobStore,
      ConnectionProvider connectionProvider,
      ICPSRDataTransferClientFactory clientFactory) {
    this.monitor = monitor;
    this.jobStore = jobStore;
    this.connectionProvider = connectionProvider;
    this.clientFactory = clientFactory;
  }

  @Override
  public ImportResult importItem(
      UUID jobId,
      IdempotentImportExecutor idempotentExecutor,
      TokenSecretAuthData authData,
      VideosContainerResource data)
      throws Exception {
    if (data == null) {
      return ImportResult.OK;
    }

    ICPSRDataTransferClient client = clientFactory.getOrCreateTransferClient(jobId, authData);

    if (data.getVideos() != null && data.getVideos().size() > 0) {
      for (VideoModel video : data.getVideos()) {
        idempotentExecutor.importAndSwallowIOExceptions(
            video,
            v -> importSingleVideo(jobId, client, v));
      }
    }

    return ImportResult.OK;
  }

  private ItemImportResult<String> importSingleVideo(
      UUID jobId,
      ICPSRDataTransferClient b2Client,
      VideoModel video)
      throws IOException {
    try (InputStream videoFileStream =
        connectionProvider.getInputStreamForItem(jobId, video).getStream()) {
      File file = jobStore
          .getTempFileFromInputStream(videoFileStream, video.getDataId(), ".mp4");
      String res = b2Client.uploadFile(
          String.format("%s/%s.mp4", VIDEO_TRANSFER_MAIN_FOLDER, video.getDataId()),
          file);
      return ItemImportResult.success(res, file.length());
    } catch (FileNotFoundException e) {
      monitor.info(
          () -> String.format("Video resource was missing for id: %s", video.getDataId()), e);
      return ItemImportResult.error(e, null);
    }
  }
}
