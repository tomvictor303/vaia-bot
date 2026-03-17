import { LogRunsService } from '../services/log/logRunsService.js';
import { LogRunEventsService } from '../services/log/logRunEventsService.js';
import { LogPagesService } from '../services/log/logPagesService.js';
import { LogCategoriesService } from '../services/log/logCategoriesService.js';
import { LogFailuresService } from '../services/log/logFailuresService.js';

const INITIAL_STAGE = 'scrape';

export class Logger {
  constructor(runId, hotelUuid, stage = '') {
    this.runId = runId;
    this.hotelUuid = hotelUuid;
    this.stage = stage;
  }

  async markStage(stage) {
    this.stage = stage || this.stage;
    return LogRunsService.logMarkStage(this.runId, stage);
  }

  async event(eventType, payload = null) {
    // payload is reserved for future event payload logging
    void payload;
    return LogRunEventsService.logRunEvent(this.runId, this.hotelUuid, this.stage || '', eventType);
  }

  async updateRun(data) {
    if (data && typeof data.stage === 'string' && data.stage) {
      this.stage = data.stage;
    }
    return LogRunsService.updateById(this.runId, data);
  }

  async pageLog(pageUrl, payload = {}) {
    // In logging systems, append-only is the default, and updates are relatively rare.
    // This method intentionally uses **insert-only mode**.
    return LogPagesService.saveLog(this.runId, this.hotelUuid, pageUrl, payload, true);
  }

  async updatePageLog(pageUrl, patch = {}) {
    return LogPagesService.saveLog(this.runId, this.hotelUuid, pageUrl, patch);
  }

  async categoryLog(categoryName, payload = {}) {
    return LogCategoriesService.saveLog(this.runId, this.hotelUuid, categoryName, payload, true);
  }

  async updateCategoryLog(categoryName, patch = {}) {
    return LogCategoriesService.saveLog(this.runId, this.hotelUuid, categoryName, patch);
  }

  async fail(stage, error) {
    this.stage = stage || this.stage;
    const errorMessage = error?.message || String(error);
    const errorClass = error?.error_class || 'system_error';
    await LogFailuresService.logFailure(this.runId, this.hotelUuid, stage, errorClass, errorMessage);
  }
}

export async function createLogger(hotelUuid) {
  // Without runId, createLogger creates a new run log and binds this logger to that run.
  const runId = await LogRunsService.insert({
    hotel_uuid: hotelUuid,
    status: 'running',
    stage: INITIAL_STAGE,
    started_at: new Date(),
  });
  return new Logger(runId, hotelUuid, INITIAL_STAGE);
}

export async function loadLogger(runId) {
  const runRow = await LogRunsService.getById(runId);
  if (!runRow) {
    throw new Error(`loadLogger cannot find run id ${runId}`);
  }
  return new Logger(runId, runRow.hotel_uuid, runRow.stage || INITIAL_STAGE);
}
