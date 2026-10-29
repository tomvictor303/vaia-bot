import { executeQuery } from '../config/database.js';

export class HotelService {
  
  // Get all active hotels from hotel_list table
  static async getActiveHotels() {
    const query = `
      SELECT id, hotel_uuid, name, hotel_url, crawl_scope, crawl_cycle, crawl_offset_days
      FROM hotel_list 
      WHERE is_market_data_capture_enabled = 1 AND is_deleted = 0 
      ORDER BY id ASC
    `;
    
    try {
      const hotels = await executeQuery(query);
      console.log(`📋 Found ${hotels.length} active hotels to process`);
      return hotels;
    } catch (error) {
      console.error('Error fetching hotels:', error.message);
      throw error;
    }
  }

  // Get hotel by UUID
  static async getHotelByUuid(hotelUuid) {
    const query = `
      SELECT id, hotel_uuid, name, hotel_url, crawl_scope, crawl_cycle, crawl_offset_days
      FROM hotel_list 
      WHERE hotel_uuid = ? AND is_deleted = 0
    `;
    
    try {
      const [hotel] = await executeQuery(query, [hotelUuid]);
      return hotel;
    } catch (error) {
      console.error('Error fetching hotel by UUID:', error.message);
      throw error;
    }
  }

  static async getAdditionalUrls(hotelId) {
    const rows = await executeQuery(`
      SELECT url
      FROM hotel_additional_urls
      WHERE hotel_id = ? AND is_enabled = 1
      ORDER BY id ASC
    `, [hotelId]);
    return rows.map(row => row.url);
  }
}
