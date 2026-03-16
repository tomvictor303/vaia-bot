import { executeQuery } from '../../config/database.js';
import { T } from '../../middleware/constants.js';
import { LogRunsService } from './logRunsService.js';

const TABLE = 'market_data_log_pages';

export class LogPagesService {
  static TABLE = TABLE;
  static RUNS_TABLE = LogRunsService.TABLE;

  static COMMON_MUTABLE_FIELDS = [
    { name: 'page_depth', type: T.NUMBER },
    { name: 'scrape_status', type: T.TEXT },
    { name: 'markdown_hash', type: T.TEXT },
    { name: 'markdown_size', type: T.NUMBER },
    { name: 'extraction_status', type: T.TEXT },
    { name: 'total_tokens', type: T.NUMBER },
    { name: 'duration_ms', type: T.NUMBER },
    { name: 'error_message', type: T.TEXT },
    { name: 'created_at', type: T.TIMESTAMP },
  ];

  static INSERTABLE_FIELDS = [
    { name: 'run_id', type: T.NUMBER },
    { name: 'hotel_uuid', type: T.TEXT },
    { name: 'page_url', type: T.TEXT },
    ...this.COMMON_MUTABLE_FIELDS,
  ];

  static UPDATABLE_FIELDS = [...this.COMMON_MUTABLE_FIELDS];

  /**
   * Insert one page log row.
   * Required: run_id, hotel_uuid, page_url
   * Optional: other fields from INSERTABLE_FIELDS.
   * @param {Object} payload
   * @returns {Promise<number>} inserted row id
   */
  static async insert(payload = {}) {
    const base = { ...payload };
    if (!base.run_id) throw new Error(`${TABLE}.insert requires run_id`);
    if (!base.hotel_uuid) throw new Error(`${TABLE}.insert requires hotel_uuid`);
    if (!base.page_url) throw new Error(`${TABLE}.insert requires page_url`);

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

  /**
   * Upsert page log by (run_id, hotel_uuid, page_url).
   * - If a row exists, apply partial update and return existing id.
   * - If not found, insert a new row and return inserted id.
   * @param {number} run_id
   * @param {string} hotelUuid
   * @param {string} page_url
   * @param {Object} patch
   * @returns {Promise<number>} row id
   */
  static async saveLog(run_id, hotelUuid, page_url, patch = {}) {
    if (!run_id) throw new Error(`${TABLE}.saveLog requires run_id`);
    if (!hotelUuid) throw new Error(`${TABLE}.saveLog requires hotelUuid`);
    if (!page_url) throw new Error(`${TABLE}.saveLog requires page_url`);

    const findQuery = `
      SELECT id
      FROM ${TABLE}
      WHERE run_id = ? AND hotel_uuid = ? AND page_url = ?
      ORDER BY id DESC
      LIMIT 1
    `;
    const foundRows = await executeQuery(findQuery, [run_id, hotelUuid, page_url]);
    const existingId = foundRows?.[0]?.id || 0;

    if (existingId) {
      await this.updateById(existingId, patch);
      return existingId;
    }

    return this.insert({
      run_id,
      hotel_uuid: hotelUuid,
      page_url,
      ...patch,
    });
  }
}
