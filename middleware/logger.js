import { LogRunsService, logMarkStage } from '../services/log/logRunsService.js';
import { logRunEvent } from '../services/log/logRunEventsService.js';
import { LogPagesService } from '../services/log/logPagesService.js';

export function createLogger({ runId, hotelUuid }) {
  return {
    async markStage(stage) {
      return logMarkStage(runId, stage);
    },

    async event(eventType, payload = null) {
      // payload is reserved for future event payload logging
      void payload;
      const stage = String(eventType || '').split('.')[0] || '';
      return logRunEvent(runId, hotelUuid, stage, eventType);
    },

    async updateRun(data) {
      return LogRunsService.updateById(runId, data);
    },

    async updatePageLog(pageUrl, patch = {}) {
      return LogPagesService.saveLog(runId, hotelUuid, pageUrl, patch);
    },

    async pageLog(pageUrl, payload = {}) {
      return LogPagesService.saveLog(runId, hotelUuid, pageUrl, payload, true);
    },

    async fail(stage, error) {
      const errorMessage = error?.message || String(error);
      await LogRunsService.updateById(runId, {
        status: 'fail',
        stage,
        error_message: errorMessage,
        finished_at: new Date(),
      });
      await logRunEvent(runId, hotelUuid, stage, `${stage}.failed`);
    },
  };
}
