import 'dotenv/config';
import { executeQuery } from '../config/database.js';
import { MD_ALL_FIELDS, BOOLEAN_FIELDS } from '../middleware/constants.js';

// Get table name from environment variable, default to 'market_data'
const MARKET_DATA_TABLE = process.env.MARKET_DATA_TABLE || 'market_data';
const MARKET_DATA_DEBUG1_TABLE = process.env.MARKET_DATA_DEBUG1_TABLE || 'market_data_debug1';

export class MarketDataService {
  
  // Boolean fields that need conversion from string to integer
  static booleanKeysList = [...BOOLEAN_FIELDS];
  
  /**
   * Convert boolean string values to integers for database storage
   * Only processes boolean fields that are actually present in the input data
   * @param {Object} data - Data object to process
   * @returns {Object} Data object with boolean values converted
   */
  static convertBooleanValues(data) {
    const convertedData = { ...data };
    
    // Only process boolean fields that are actually present in the data
    this.booleanKeysList.forEach(key => {
      // Skip if this key is not in the input data
      // This is **IMPORTANT** to prevent unintentional data fields in insert/update
      if (!(key in data)) {
        return;
      }
      
      const value = convertedData[key];
      
      // Handle boolean values
      if (typeof value === 'boolean') {
        convertedData[key] = value ? 1 : 0;
      }
      // Handle number values (0 or 1)
      else if (typeof value === 'number') {
        convertedData[key] = value === 0 ? 0 : (value === 1 ? 1 : null);
      }
      // Handle string values
      else if (typeof value === 'string') {
        const flatValue = value.toLowerCase().replaceAll(' ', '');
        if (flatValue === 'true' || flatValue === 'yes' || flatValue === 'ok' || flatValue === 'y' || flatValue === '1') {
          convertedData[key] = 1;
        } else if (flatValue === 'false' || flatValue === 'no' || flatValue === 'n' || flatValue === '0') {
          convertedData[key] = 0;
        } else if (flatValue === '' || flatValue === 'n/a') {
          convertedData[key] = null;
        } else {
          convertedData[key] = null; // If not sure about the value, set to null
        }
      }
      // Handle null/undefined - keep as is (already null or will be undefined)
      else if (value === undefined || value === null) {
        convertedData[key] = null;
      }
      // Unknown type - set to null
      else {
        convertedData[key] = null;
      }
    });
    
    return convertedData;
  }
  
  /**
   * Get market data ID by hotel_uuid
   * Returns ID if exists, 0 if not exists
   * @param {string} hotelUuid - The hotel UUID to search for
   * @returns {Promise<number>} The market data ID if exists, 0 if not exists
   * @throws {Error} When database query fails
   */
  static async getIdByUuid(hotelUuid) {
    const query = `
      SELECT id FROM ${MARKET_DATA_TABLE} 
      WHERE hotel_uuid = ? AND is_deleted = 0
    `;
    
    try {
      const [result] = await executeQuery(query, [hotelUuid]);
      return result ? result.id : 0;
    } catch (error) {
      console.error('Error getting market data ID by UUID:', error.message);
      return 0;
    }
  }

  /**
   * Get existing market data by hotel_uuid
   * @param {string} hotelUuid - The hotel UUID to search for
   * @returns {Promise<Object|null>} The market data object or null if not found
   * @throws {Error} When database query fails
   */
  static async getMarketDataByUuid(hotelUuid) {
    const query = `
      SELECT * FROM ${MARKET_DATA_TABLE} 
      WHERE hotel_uuid = ? AND is_deleted = 0
    `;
    
    try {
      const [result] = await executeQuery(query, [hotelUuid]);
      return result;
    } catch (error) {
      console.error('Error getting market data by UUID:', error.message);
      return null;
    }
  }

  /**
   * Get list of empty fields from existing market data
   * @param {Object} existingData - Existing market data object
   * @param {Array} allFields - Array of all field names to check
   * @returns {Array<string>} Array of field names that are empty or null
   */
  static getEmptyFields(existingData, allFields) {
    if (!existingData) {
      return allFields;
    }
    
    return allFields.filter(fieldName => {
      const value = existingData[fieldName];
      return value === null || value === undefined || value === '' || value === 'N/A';
    });
  }

  /**
   * Merge new data with existing data, only updating non-empty values
   * Prevents overwriting existing data with empty values
   * @param {Object} existingData - Existing market data
   * @param {Object} newData - New data to merge
   * @returns {Object} Merged data object
   */
  static mergeDataSafely(existingData, newData) {
    const merged = { ...existingData };
    
    // Only update fields that have non-empty values in newData
    Object.keys(newData).forEach(key => {
      const newValue = newData[key];
      // Only update if new value is not empty/null/N/A
      if (newValue !== null && newValue !== undefined && newValue !== '' && newValue !== 'N/A') {
        merged[key] = newValue;
      }
    });
    
    return merged;
  }

  /**
   * Filter data object to only include fields that are in MD_ALL_FIELDS
   * Prevents SQL crashes from invalid field names
   * @param {Object} data - Data object to filter
   * @returns {Object} Filtered data object containing only valid fields
   */
  static filterValidFields(data) {
    const validFieldNamesSet = new Set(MD_ALL_FIELDS.map(f => f.name));
    const otherTableFieldNames4Upsert = ['hotel_uuid'];
    const filteredData = {};
    
    Object.keys(data).forEach(key => {
      if (validFieldNamesSet.has(key) || otherTableFieldNames4Upsert.includes(key)) {
        filteredData[key] = data[key];
      }
    });
    
    return filteredData;
  }

  /**
   * Insert new market data into the database
   * @param {Object} data - Market data object containing hotel information (can be partial)
   * @param {string} data.hotel_uuid - Unique identifier for the hotel
   * @returns {Promise<number>} The insert ID of the newly created market data record
   * @throws {Error} When database insertion fails
   * @example
   * const marketData = {
   *   hotel_uuid: 'uuid-123',
   *   name: 'Grand Hotel',
   *   description: 'A luxurious hotel in the heart of the city',
   *   amenities: 'Pool,Spa,Restaurant,WiFi',
   *   email: 'info@grandhotel.com'
   * };
   * const insertId = await MarketDataService.insertMarketData(marketData);
   * console.log(`Market data inserted with ID: ${insertId}`);
   */
  static async insertMarketData(data) {
    // Filter out invalid fields first
    const filteredData = this.filterValidFields(data);
    
    // Convert boolean values before processing
    const fineData = this.convertBooleanValues(filteredData);

    // Build columns array from fields that actually exist in the filtered data
    // Since fineData is already filtered to only contain valid fields, we can use Object.keys directly
    const columns = Object.keys(fineData);
    
    if (columns.length === 0) {
      throw new Error('No valid fields provided for insertion');
    }
    
    const placeholders = columns.map(() => '?').join(', ');
    const query = `
      INSERT INTO ${MARKET_DATA_TABLE} (${columns.join(', ')})
      VALUES (${placeholders})
    `;

    const values = columns.map((col) => {
      const v = fineData[col];
      return v ?? null;
    });
    
    try {
      const result = await executeQuery(query, values);
      console.log(`✅ Market data inserted successfully. ID: ${result.insertId}`);
      return result.insertId;
    } catch (error) {
      console.error('Error inserting market data:', error.message);
      throw error;
    }
  }

  /**
   * Update existing market data in the database by ID
   * We do not update hotel_uuid here
   * @param {number} id - The database ID of the market data record to update
   * @param {Object} data - Market data object containing updated information (can be partial)
   * @returns {Promise<number>} The number of affected rows (should be 1 if successful)
   * @throws {Error} When database update fails or market data with given ID doesn't exist
   * @example
   * const updatedData = {
   *   name: 'Grand Hotel Updated',
   *   description: 'An updated luxurious hotel description',
   *   main_phone: '+1-555-0124'
   * };
   * const affectedRows = await MarketDataService.updateMarketData(123, updatedData);
   * console.log(`Updated ${affectedRows} market data record(s)`);
   */
  static async updateMarketData(id, data) {
    // Filter out invalid fields first (exclude hotel_uuid for safety)
    const { hotel_uuid, ...dataWithoutUuid } = data;
    const filteredData = this.filterValidFields(dataWithoutUuid);
    
    // Convert boolean values before processing
    const fineData = this.convertBooleanValues(filteredData);
    
    // If no valid fields to update, return early
    const columns = Object.keys(fineData);
    if (columns.length === 0) {
      console.log('⚠️ No valid fields to update');
      return 0;
    }
    
    // Only update fields that are actually in the filtered data
    const setClause = columns.map((f) => `${f} = ?`).join(', ');
    const query = `
      UPDATE ${MARKET_DATA_TABLE} SET ${setClause}, updated_at = CURRENT_TIMESTAMP
      WHERE id = ?
    `;

    const values = [
      ...columns.map((col) => {
        const v = fineData[col];
        return v ?? null;
      }),
      id
    ];
    
    try {
      const result = await executeQuery(query, values);
      console.log(`✅ Market data updated successfully. Affected rows: ${result.affectedRows}`);
      return result.affectedRows;
    } catch (error) {
      console.error('Error updating market data:', error.message);
      throw error;
    }
  }

  /**
   * Upsert market data (insert or update based on hotel_uuid)
   * @param {Object} data - Market data object containing all hotel information
   * @param {string} hotelUuid - The hotel UUID to use for upsert operation
   * @returns {Promise<Object>} Object containing action type and result data
   * @returns {Promise<Object>} Returns {action: 'insert', insertId: number} for new records
   * @returns {Promise<Object>} Returns {action: 'update', affectedRows: number, id: number} for existing records
   * @throws {Error} When database operation fails
   * @example
   * const marketData = {
   *   name: 'Grand Hotel',
   *   location: 'New York, NY',
   *   description: 'A luxurious hotel'
   * };
   * const result = await MarketDataService.upsertMarketData(marketData, 'uuid-123');
   * if (result.action === 'insert') {
   *   console.log(`Inserted with ID: ${result.insertId}`);
   * } else {
   *   console.log(`Updated ${result.affectedRows} rows`);
   * }
   */
  static async upsertMarketData(data, hotelUuid) {
    try {
      const existingId = await this.getIdByUuid(hotelUuid);
      
      if (existingId > 0) {
        console.log(`🔄 Updating existing market data for UUID: ${hotelUuid} (ID: ${existingId})`);
        const affectedRows = await this.updateMarketData(existingId, data);
        return { action: 'update', affectedRows, id: existingId };
      } else {
        console.log(`➕ Inserting new market data for UUID: ${hotelUuid}`);
        // Add hotel_uuid to data object for insert
        const dataWithUuid = { ...data, hotel_uuid: hotelUuid };
        const insertId = await this.insertMarketData(dataWithUuid);
        return { action: 'insert', insertId };
      }
    } catch (error) {
      console.error('Error upserting market data:', error.message);
      throw error;
    }
  }

  /**
   * Upsert into market_data_debug1 (same structure as market_data).
   * Each field value is expected to be a JSON string or null (e.g. JSON.stringify({ snippets, newData })).
   * @param {Object} debugData - Object keyed by field name, values are JSON strings or null
   * @param {string} hotelUuid - Hotel UUID for upsert key
   * @returns {Promise<Object>} { action: 'insert'|'update', insertId?: number, affectedRows?: number, id?: number }
   */
  static async upsertMarketDataDebug1(debugData, hotelUuid) {
    if (Object.keys(debugData).length === 0) {
      return { action: 'update', affectedRows: 0, id: 0 };
    }
    const filteredData = this.filterValidFields(debugData);

    // Inline check: existing record by hotel_uuid
    const existingQuery = `
      SELECT id FROM ${MARKET_DATA_DEBUG1_TABLE}
      WHERE hotel_uuid = ? AND is_deleted = 0
    `;
    let existingId = 0;
    try {
      const [row] = await executeQuery(existingQuery, [hotelUuid]);
      existingId = row ? row.id : 0;
    } catch (err) {
      console.error('Error checking existing debug record:', err.message);
      throw err;
    }
   
    // BEGIN UPSERT_DEBUG_RECORD
    if (existingId > 0) {
      // BEGIN UPDATE_DEBUG_RECORD
      const columns = Object.keys(filteredData);
      if (columns.length === 0) {
        return { action: 'update', affectedRows: 0, id: existingId };
      }
      const setClause = columns.map((f) => `${f} = ?`).join(', ');
      const updateQuery = `
        UPDATE ${MARKET_DATA_DEBUG1_TABLE} SET ${setClause}, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
      `;
      const values = [...columns.map((col) => filteredData[col] ?? null), existingId];
      const result = await executeQuery(updateQuery, values);
      return { action: 'update', affectedRows: result.affectedRows, id: existingId };
      // END UPDATE_DEBUG_RECORD
    } else {
      // BEGIN INSERT_DEBUG_RECORD
      const dataWithUuid = { ...filteredData, hotel_uuid: hotelUuid };
      const columns = Object.keys(dataWithUuid);
      const placeholders = columns.map(() => '?').join(', ');
      const insertQuery = `
        INSERT INTO ${MARKET_DATA_DEBUG1_TABLE} (${columns.join(', ')})
        VALUES (${placeholders})
      `;
      const values = columns.map((col) => dataWithUuid[col] ?? null);
      const result = await executeQuery(insertQuery, values);
      return { action: 'insert', insertId: result.insertId };
      // END INSERT_DEBUG_RECORD
    }    
    // END UPSERT_DEBUG_RECORD
  }
}
