import { executeQuery } from '../../config/database.js';
import { T } from '../../middleware/constants.js';

const TABLE = 'market_data_log_run_events';

export class LogRunEventsService {
  static TABLE = TABLE;

  static INSERTABLE_FIELDS = [
    { name: 'run_id', type: T.NUMBER },
    { name: 'hotel_uuid', type: T.TEXT },
    { name: 'stage', type: T.TEXT },
    { name: 'event_type', type: T.TEXT },
    { name: 'message', type: T.TEXT },
    // Stored to JSON column; accepts JSON string.
    { name: 'payload_json', type: T.TEXT },
    { name: 'created_at', type: T.TIMESTAMP },
  ];

  /**
   * Insert one run-event row.
   * Required: run_id, hotel_uuid, stage, event_type
   * Optional: message, payload_json, created_at
   * @param {Object} payload
   * @returns {Promise<number>} inserted row id
   */
  static async insert(payload = {}) {
    const base = { ...payload };
    if (!base.run_id) throw new Error(`${TABLE}.insert requires run_id`);
    if (!base.hotel_uuid) throw new Error(`${TABLE}.insert requires hotel_uuid`);
    if (!base.stage) throw new Error(`${TABLE}.insert requires stage`);
    if (!base.event_type) throw new Error(`${TABLE}.insert requires event_type`);

    // MySQL JSON column: send a JSON string if caller passes object/array.
    if (base.payload_json != null && typeof base.payload_json === 'object') {
      base.payload_json = JSON.stringify(base.payload_json);
    }

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
   * Safe event logger wrapper for run-event inserts.
   * @param {number} runId
   * @param {string} hotelUuid
   * @param {string} stage
   * @param {string} eventType
   * @returns {Promise<number>} inserted row id (0 on failure)
   */
  static async logRunEvent(runId, hotelUuid, stage, eventType) {
    try {
      return await LogRunEventsService.insert({
        run_id: runId,
        hotel_uuid: hotelUuid,
        stage,
        event_type: eventType,
      });
    } catch (error) {
      console.error(`⚠️ Failed to log run event "${eventType}" for run ${runId}:`, error.message);
      return 0;
    }
  }
}
