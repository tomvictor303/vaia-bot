import { LogRunsService } from '../services/log/logRunsService.js';
import { LogRunEventsService } from '../services/log/logRunEventsService.js';
import { LogPagesService } from '../services/log/logPagesService.js';
import { LogCategoriesService } from '../services/log/logCategoriesService.js';
import { LogFailuresService } from '../services/log/logFailuresService.js';
import { ERROR_CLASS } from './constants.js';

const INITIAL_STAGE = 'scrape';

function autoClassifyErrorClass(error) {
  const msg = String(error?.message || error || '').toLowerCase();
  if (/timeout|timed out|etimedout/.test(msg)) return ERROR_CLASS.TIMEOUT;
  if (/invalid|required|must be|validation/.test(msg)) return ERROR_CLASS.VALIDATION_ERROR;
  if (/llm|openai|model|token/.test(msg)) return ERROR_CLASS.LLM_ERROR;
  if (/parse|parsing|json/.test(msg)) return ERROR_CLASS.PARSING_ERROR;
  if (/sql|mysql|database|query|db/.test(msg)) return ERROR_CLASS.DB_ERROR;
  return ERROR_CLASS.SYSTEM_ERROR;
}

export class Logger {
  constructor(runId, hotelUuid, stage = '') {
    this.runId = runId;
    this.hotelUuid = hotelUuid;
    this.stage = stage;
  }

  async reload() {
    if (!this.runId) {
      throw new Error('Logger.reload requires runId');
    }
    const runRow = await LogRunsService.getById(this.runId);
    if (!runRow) {
      throw new Error(`Logger.reload cannot find run id ${this.runId}`);
    }
    this.hotelUuid = runRow.hotel_uuid;
    this.stage = runRow.stage || INITIAL_STAGE;
    return this;
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

  async fail(error, error_class = null) {
    const errorMessage = error?.message || String(error);
    const errorClass = error_class || autoClassifyErrorClass(error);
    const failureStage = this.stage || INITIAL_STAGE;
    await LogFailuresService.logFailure(this.runId, this.hotelUuid, failureStage, errorClass, errorMessage);
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
