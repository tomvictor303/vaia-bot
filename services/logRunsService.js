import { executeQuery } from '../config/database.js';
import { T } from '../middleware/constants.js';

const TABLE = 'market_data_log_runs';

export class LogRunsService {
  static COMMON_MUTABLE_FIELDS = [
    { name: 'status', type: T.TEXT },
    { name: 'stage', type: T.TEXT },
    { name: 'started_at', type: T.TIMESTAMP },
    { name: 'finished_at', type: T.TIMESTAMP },
    { name: 'duration_ms', type: T.NUMBER },
    { name: 'pages_scraped', type: T.NUMBER },
    { name: 'pages_analyzed', type: T.NUMBER },
    { name: 'categories_updated', type: T.NUMBER },
    { name: 'tokens_used', type: T.NUMBER },
    { name: 'cost', type: T.NUMBER },
    { name: 'model_version', type: T.TEXT },
    { name: 'prompt_version', type: T.TEXT },
    { name: 'error_message', type: T.TEXT },
  ];

  static INSERTABLE_FIELDS = [{ name: 'hotel_uuid', type: T.TEXT }, ...this.COMMON_MUTABLE_FIELDS];

  static UPDATABLE_FIELDS = [...this.COMMON_MUTABLE_FIELDS];

  /**
   * Insert one run log row.
   * Required: hotel_uuid, status, stage
   * Optional: all other fields; started_at defaults to current timestamp if omitted.
   * @param {Object} payload
   * @returns {Promise<number>} inserted row id
   */
  static async insert(payload = {}) {
    const base = { ...payload };
    if (!base.hotel_uuid) throw new Error(`${TABLE}.insert requires hotel_uuid`);
    if (!base.status) throw new Error(`${TABLE}.insert requires status`);
    if (!base.stage) throw new Error(`${TABLE}.insert requires stage`);

    if (!base.started_at) {
      base.started_at = new Date();
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
   * Update (partial or full) by id.
   * Only keys in UPDATABLE_FIELDS are applied.
   * @param {number} id
   * @param {Object} patch
   * @returns {Promise<number>} affected rows
   */
  static async updateById(id, patch = {}) {
    if (!id) throw new Error(`${TABLE}.updateById requires id`);

    const fields = this.UPDATABLE_FIELDS
      .map((f) => f.name)
      .filter(
        (name) => Object.prototype.hasOwnProperty.call(patch, name) && patch[name] !== undefined
    );
    if (fields.length === 0) return 0;

    const setClause = fields.map((name) => `${name} = ?`).join(', ');
    const params = fields.map((name) => patch[name]);
    params.push(id);

    const query = `
      UPDATE ${TABLE}
      SET ${setClause}
      WHERE id = ?
    `;

    const result = await executeQuery(query, params);
    return result.affectedRows || 0;
  }
}
