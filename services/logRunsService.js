import { executeQuery } from '../config/database.js';

const TABLE = 'market_data_log_runs';

export class LogRunsService {
  static COMMON_MUTABLE_FIELDS = [
    'status',
    'stage',
    'started_at',
    'finished_at',
    'duration_ms',
    'pages_scraped',
    'pages_analyzed',
    'categories_updated',
    'tokens_used',
    'cost',
    'model_version',
    'prompt_version',
    'error_message',
  ];

  static INSERTABLE_FIELDS = ['hotel_uuid', ...this.COMMON_MUTABLE_FIELDS];

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
    if (!base.hotel_uuid) throw new Error('logRuns.insert requires hotel_uuid');
    if (!base.status) throw new Error('logRuns.insert requires status');
    if (!base.stage) throw new Error('logRuns.insert requires stage');

    if (!base.started_at) {
      base.started_at = new Date();
    }

    const fields = this.INSERTABLE_FIELDS.filter((f) => Object.prototype.hasOwnProperty.call(base, f));
    const values = fields.map((f) => base[f]);
    const placeholders = fields.map(() => '?').join(', ');

    const query = `
      INSERT INTO ${TABLE} (${fields.join(', ')})
      VALUES (${placeholders})
    `;

    const result = await executeQuery(query, values);
    return result.insertId || 0;
  }

  /**
   * Partial update by log run id.
   * Only keys in UPDATABLE_FIELDS are applied.
   * @param {number} id
   * @param {Object} patch
   * @returns {Promise<number>} affected rows
   */
  static async updatePartialById(id, patch = {}) {
    if (!id) throw new Error('logRuns.updatePartialById requires id');

    const fields = this.UPDATABLE_FIELDS.filter(
      (f) => Object.prototype.hasOwnProperty.call(patch, f) && patch[f] !== undefined
    );
    if (fields.length === 0) return 0;

    const setClause = fields.map((f) => `${f} = ?`).join(', ');
    const params = fields.map((f) => patch[f]);
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
