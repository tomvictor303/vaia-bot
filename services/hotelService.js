import { executeQuery } from '../config/database.js';

export class HotelService {
  
  // Get all active hotels from hotel_list table
  static async getActiveHotels() {
    const query = `
      SELECT id, hotel_uuid, name 
      FROM hotel_list 
      WHERE is_deleted = 0 
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
      SELECT id, hotel_uuid, name 
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

}
