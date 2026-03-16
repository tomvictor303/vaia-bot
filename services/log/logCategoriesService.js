import { executeQuery } from '../../config/database.js';
import { T } from '../../middleware/constants.js';
import { LogRunsService } from './logRunsService.js';

const TABLE = 'market_data_log_categories';

export class LogCategoriesService {
  static TABLE = TABLE;
  static RUNS_TABLE = LogRunsService.TABLE;

  static COMMON_MUTABLE_FIELDS = [
    { name: 'merged_text', type: T.TEXT },
    { name: 'output_hash', type: T.TEXT },
    { name: 'snippets_count', type: T.NUMBER },
    { name: 'is_updated', type: T.BOOLEAN },
    { name: 'total_tokens', type: T.NUMBER },
    { name: 'duration_ms', type: T.NUMBER },
    { name: 'created_at', type: T.TIMESTAMP },
  ];

  static INSERTABLE_FIELDS = [
    { name: 'run_id', type: T.NUMBER },
    { name: 'hotel_uuid', type: T.TEXT },
    { name: 'category_name', type: T.TEXT },
    ...this.COMMON_MUTABLE_FIELDS,
  ];

  static UPDATABLE_FIELDS = [...this.COMMON_MUTABLE_FIELDS];

  /**
   * Insert one category log row.
   * Required: run_id, hotel_uuid, category_name
   * Optional: other fields from INSERTABLE_FIELDS.
   * @param {Object} payload
   * @returns {Promise<number>} inserted row id
   */
  static async insert(payload = {}) {
    const base = { ...payload };
    if (!base.run_id) throw new Error(`${TABLE}.insert requires run_id`);
    if (!base.hotel_uuid) throw new Error(`${TABLE}.insert requires hotel_uuid`);
    if (!base.category_name) throw new Error(`${TABLE}.insert requires category_name`);

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
   * Upsert category log by (run_id, hotel_uuid, category_name).
   * - If insertOnlyMode=true, always insert a new row (no select/update).
   * - If a row exists, apply partial update and return existing id.
   * - If not found, insert a new row and return inserted id.
   * @param {number} run_id
   * @param {string} hotelUuid
   * @param {string} category_name
   * @param {Object} patch
   * @param {boolean} [insertOnlyMode=false]
   * @returns {Promise<number>} row id
   */
  static async saveLog(run_id, hotelUuid, category_name, patch = {}, insertOnlyMode = false) {
    try {
      if (!run_id) throw new Error(`${TABLE}.saveLog requires run_id`);
      if (!hotelUuid) throw new Error(`${TABLE}.saveLog requires hotelUuid`);
      if (!category_name) throw new Error(`${TABLE}.saveLog requires category_name`);

      if (!insertOnlyMode) {
        const findQuery = `
          SELECT id
          FROM ${TABLE}
          WHERE run_id = ? AND hotel_uuid = ? AND category_name = ?
          ORDER BY id DESC
          LIMIT 1
        `;
        const foundRows = await executeQuery(findQuery, [run_id, hotelUuid, category_name]);
        const existingId = foundRows?.[0]?.id || 0;

        if (existingId) {
          await this.updateById(existingId, patch);
          return existingId;
        }
      }

      return this.insert({
        run_id,
        hotel_uuid: hotelUuid,
        category_name,
        ...patch,
      });
    } catch (error) {
      console.error(`⚠️ Failed to save category log for run ${run_id} (${category_name}):`, error.message);
      return 0;
    }
  }
}
