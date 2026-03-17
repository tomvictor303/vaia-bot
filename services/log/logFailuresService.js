import { executeQuery } from '../../config/database.js';
import { T } from '../../middleware/constants.js';

const TABLE = 'market_data_log_failures';

/**
 * Safe failure logger wrapper for failure inserts.
 * @param {number} runId
 * @param {string} hotelUuid
 * @param {string} stage
 * @param {string} errorClass
 * @param {string} errorMessage
 * @param {string|null} [pageUrl=null]
 * @param {string|null} [categoryName=null]
 * @returns {Promise<number>} inserted row id (0 on failure)
 */
export async function logFailure(
  runId,
  hotelUuid,
  stage,
  errorClass,
  errorMessage,
  pageUrl = null,
  categoryName = null
) {
  try {
    return await LogFailuresService.insert({
      run_id: runId,
      hotel_uuid: hotelUuid,
      stage,
      error_class: errorClass,
      error_message: errorMessage,
      page_url: pageUrl,
      category_name: categoryName,
    });
  } catch (error) {
    console.error(`⚠️ Failed to log failure "${errorClass}" for run ${runId}:`, error.message);
    return 0;
  }
}

export class LogFailuresService {
  static TABLE = TABLE;

  static INSERTABLE_FIELDS = [
    { name: 'run_id', type: T.NUMBER },
    { name: 'hotel_uuid', type: T.TEXT },
    { name: 'stage', type: T.TEXT },
    { name: 'error_class', type: T.TEXT },
    { name: 'page_url', type: T.TEXT },
    { name: 'category_name', type: T.TEXT },
    { name: 'error_message', type: T.TEXT },
    { name: 'created_at', type: T.TIMESTAMP },
  ];

  /**
   * Insert one failure row.
   * Required: run_id, hotel_uuid, stage, error_class, error_message
   * Optional: page_url, category_name, created_at
   * @param {Object} payload
   * @returns {Promise<number>} inserted row id
   */
  static async insert(payload = {}) {
    const base = { ...payload };
    if (!base.run_id) throw new Error(`${TABLE}.insert requires run_id`);
    if (!base.hotel_uuid) throw new Error(`${TABLE}.insert requires hotel_uuid`);
    if (!base.stage) throw new Error(`${TABLE}.insert requires stage`);
    if (!base.error_class) throw new Error(`${TABLE}.insert requires error_class`);
    if (!base.error_message) throw new Error(`${TABLE}.insert requires error_message`);

    const fields = this.INSERTABLE_FIELDS
      .map((f) => f.name)
      .filter((name) => Object.prototype.hasOwnProperty.call(base, name));
    const values = fields.map((name) => base[name]);
    const placeholders = fields.map(() => '?').join(', ');

    const query = `
      INSERT INTO ${TABLE} (${fields.join(', ')})
      VALUES (${placeholders})
    `;

    const result = await executeQuery(query, values);
    return result.insertId || 0;
  }

  /**
   * Safe failure logger wrapper for failure inserts.
   * @param {number} runId
   * @param {string} hotelUuid
   * @param {string} stage
   * @param {string} errorClass
   * @param {string} errorMessage
   * @param {string|null} [pageUrl=null]
   * @param {string|null} [categoryName=null]
   * @returns {Promise<number>} inserted row id (0 on failure)
   */
  static async logFailure(
    runId,
    hotelUuid,
    stage,
    errorClass,
    errorMessage,
    pageUrl = null,
    categoryName = null
  ) {
    return logFailure(runId, hotelUuid, stage, errorClass, errorMessage, pageUrl, categoryName);
  }
}
