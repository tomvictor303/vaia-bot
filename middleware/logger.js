import { LogRunsService } from '../services/log/logRunsService.js';
import { LogRunEventsService } from '../services/log/logRunEventsService.js';
import { LogPagesService } from '../services/log/logPagesService.js';
import { LogCategoriesService } from '../services/log/logCategoriesService.js';
import { LogFailuresService } from '../services/log/logFailuresService.js';

export async function createLogger({ runId = null, hotelUuid }) {
  let myRunId = runId;
  if (!myRunId) {
    // Without runId, createLogger creates a new run log and binds this logger to that run.
    myRunId = await LogRunsService.insert({
      hotel_uuid: hotelUuid,
      status: 'running',
      stage: 'scrape',
      started_at: new Date(),
    });
  }

  return {
    runId: myRunId,

    async markStage(stage) {
      return LogRunsService.logMarkStage(myRunId, stage);
    },

    async event(eventType, payload = null) {
      // payload is reserved for future event payload logging
      void payload;
      const stage = String(eventType || '').split('.')[0] || '';
      return LogRunEventsService.logRunEvent(myRunId, hotelUuid, stage, eventType);
    },

    async updateRun(data) {
      return LogRunsService.updateById(myRunId, data);
    },

    async pageLog(pageUrl, payload = {}) {
      // In logging systems, append-only is the default, and updates are relatively rare.
      // This method intentionally uses **insert-only mode**.
      return LogPagesService.saveLog(myRunId, hotelUuid, pageUrl, payload, true);
    },

    async updatePageLog(pageUrl, patch = {}) {
      return LogPagesService.saveLog(myRunId, hotelUuid, pageUrl, patch);
    },

    async categoryLog(categoryName, payload = {}) {
      return LogCategoriesService.saveLog(myRunId, hotelUuid, categoryName, payload, true);
    },

    async updateCategoryLog(categoryName, patch = {}) {
      return LogCategoriesService.saveLog(myRunId, hotelUuid, categoryName, patch);
    },

    async fail(stage, error) {
      const errorMessage = error?.message || String(error);
      const errorClass = error?.error_class || 'system_error';
      await LogFailuresService.logFailure(myRunId, hotelUuid, stage, errorClass, errorMessage);
    },
  };
}
