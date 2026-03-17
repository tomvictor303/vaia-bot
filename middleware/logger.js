import { LogRunsService } from '../services/log/logRunsService.js';
import { LogRunEventsService } from '../services/log/logRunEventsService.js';
import { LogPagesService } from '../services/log/logPagesService.js';
import { LogCategoriesService } from '../services/log/logCategoriesService.js';
import { LogFailuresService } from '../services/log/logFailuresService.js';

export function createLogger({ runId, hotelUuid }) {
  return {
    async markStage(stage) {
      return LogRunsService.logMarkStage(runId, stage);
    },

    async event(eventType, payload = null) {
      // payload is reserved for future event payload logging
      void payload;
      const stage = String(eventType || '').split('.')[0] || '';
      return LogRunEventsService.logRunEvent(runId, hotelUuid, stage, eventType);
    },

    async updateRun(data) {
      return LogRunsService.updateById(runId, data);
    },

    async pageLog(pageUrl, payload = {}) {
      // In logging systems, append-only is the default, and updates are relatively rare.
      // This method intentionally uses **insert-only mode**.
      return LogPagesService.saveLog(runId, hotelUuid, pageUrl, payload, true);
    },

    async updatePageLog(pageUrl, patch = {}) {
      return LogPagesService.saveLog(runId, hotelUuid, pageUrl, patch);
    },

    async categoryLog(categoryName, payload = {}) {
      return LogCategoriesService.saveLog(runId, hotelUuid, categoryName, payload, true);
    },

    async updateCategoryLog(categoryName, patch = {}) {
      return LogCategoriesService.saveLog(runId, hotelUuid, categoryName, patch);
    },

    async fail(stage, error) {
      const errorMessage = error?.message || String(error);
      const errorClass = error?.error_class || 'system_error';
      await LogFailuresService.logFailure(runId, hotelUuid, stage, errorClass, errorMessage);
    },
  };
}
