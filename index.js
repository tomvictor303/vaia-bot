import 'dotenv/config';
import { testConnection, closePool } from './config/database.js';
import { HotelService } from './services/hotelService.js';
import { MarketDataService } from './services/marketDataService.js';
import { AIService } from './services/aiService.js';
import { MD_ALL_FIELDS } from './middleware/constants.js';

/**
 * Process hotel with retry logic to fill all fields
 * @param {Object} hotel - Hotel object from database
 */
async function processHotelWithRetry(hotel) {
  const maxAttempts = Math.max(parseInt(process.env.SCRAPE_MAX_TRY || '2', 10), 1);
  const allFieldNames = MD_ALL_FIELDS.map(f => f.name);
  
  let existingData = await MarketDataService.getMarketDataByUuid(hotel.hotel_uuid);
  let accumulatedData = {}; // Start with empty object, then merge with new data
  let attempt = 0;
  
  // Attempt loop - only fetch data, don't update database
  while (attempt < maxAttempts) {
    attempt++;
    console.log(`\n📊 Attempt ${attempt}/${maxAttempts} for ${hotel.name}`);
    
    // Get empty fields
    const emptyFields = MarketDataService.getEmptyFields(accumulatedData, allFieldNames);
    
    if (emptyFields.length === 0) {
      console.log("✅ All fields are filled! No retry needed.");
      break;
    }
    
    console.log(`📋 Empty fields (${emptyFields.length}): ${emptyFields.slice(0, 5).join(', ')}${emptyFields.length > 5 ? '...' : ''}`);
    
    try {
      // Fetch only empty fields
      const newData = await AIService.fetchHotelData(hotel.name, emptyFields);
      
      // Merge new data with existing data (only non-empty values)
      accumulatedData = MarketDataService.mergeDataSafely(accumulatedData, newData);
      
      // Check if all fields are now filled
      const remainingEmptyFields = MarketDataService.getEmptyFields(accumulatedData, allFieldNames);
      if (remainingEmptyFields.length === 0) {
        console.log("🎉 All fields successfully filled!");
        break;
      }
      
      console.log(`📊 Still missing ${remainingEmptyFields.length} fields`);
      
      // Wait before next retry
      if (attempt < maxAttempts && remainingEmptyFields.length > 0) {
        console.log("⏳ Waiting 5 seconds before next retry...");
        await new Promise(resolve => setTimeout(resolve, 5000));
      }
      
    } catch (error) {
      console.error(`❌ Error on attempt ${attempt}:`, error.message);
      if (attempt >= maxAttempts) {
        // Continue to database update even if last attempt failed
        // This is user friendly message to avoid confusion.
        console.log("⚠️  Proceeding to save accumulated data despite error...");
        break;
      }
      console.log("⏳ Waiting 5 seconds before retry...");
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
  }
  
  // Update database outside of retry loop
  console.log("\n💾 Saving data to database...");
  try {
    if (existingData) {
      // Update existing record
      const existingId = await MarketDataService.getIdByUuid(hotel.hotel_uuid);
      if (existingId > 0) {
        await MarketDataService.updateMarketData(existingId, accumulatedData);
        console.log(`✅ Updated ${hotel.name} in database`);
      }
    } else {
      // Insert new record
      const dataWithUuid = { ...accumulatedData, hotel_uuid: hotel.hotel_uuid };
      const insertId = await MarketDataService.insertMarketData(dataWithUuid);
      console.log(`✅ Inserted ${hotel.name} in database (ID: ${insertId})`);
    }
  } catch (error) {
    console.error(`❌ Error saving to database:`, error.message);
    throw error;
  }
  
  // Final status
  const finalEmptyFields = MarketDataService.getEmptyFields(accumulatedData, allFieldNames);
  if (finalEmptyFields.length > 0) {
    console.log(`⚠️  Completed with ${finalEmptyFields.length} empty fields remaining after ${attempt} attempt(s)`);
  } else {
    console.log(`✅ Successfully filled all fields for ${hotel.name}`);
  }

  // Fetch FAQs after processing data (optional)
  await fetchAndPrintHotelFAQ(hotel.name);
}

/**
 * Fetch hotel FAQs and print to console
 * @param {string} hotelName
 */
async function fetchAndPrintHotelFAQ(hotelName) {
  try {
    const faqs = await AIService.fetchHotelFAQ(hotelName);
    if (!faqs || faqs.length === 0) {
      console.log(`ℹ️  No FAQs found for ${hotelName}`);
      return;
    }

    console.log(`\n📘 FAQ for ${hotelName}:`);
    faqs.forEach((faq, index) => {
      console.log(`\nQ${index + 1}: ${faq.question || 'N/A'}`);
      console.log(`A${index + 1}: ${faq.answer || 'N/A'}`);
    });
  } catch (error) {
    console.error(`⚠️  Could not fetch FAQs for ${hotelName}:`, error.message);
  }
}

async function main() {
  console.log("🚀 Starting Hotel Data Fetcher...");
  
  // Test database connection
  const isConnected = await testConnection();
  if (!isConnected) {
    console.error("❌ Cannot proceed without database connection");
    process.exit(1);
  }

  try {
    // Get all active hotels from database
    const hotels = await HotelService.getActiveHotels();
    
    if (hotels.length === 0) {
      console.log("📭 No active hotels found in database");
      return;
    }

    console.log(`\n🏨 Processing ${hotels.length} hotels...\n`);

    // Process each hotel
    for (let i = 0; i < hotels.length; i++) {
      const hotel = hotels[i];
      console.log(`\n${"=".repeat(60)}`);
      console.log(`🏨 Processing Hotel ${i + 1}/${hotels.length}: ${hotel.name}`);
      console.log(`🆔 UUID: ${hotel.hotel_uuid}`);
      console.log(`${"=".repeat(60)}\n`);

      try {
        await processHotelWithRetry(hotel);
      } catch (error) {
        console.error(`❌ Error processing ${hotel.name}:`, error.message);
        console.log("⏭️  Continuing with next hotel...");
      }

      // Add delay between hotels to be respectful to the API
      if (i < hotels.length - 1) {
        console.log("⏳ Waiting 3 seconds before next hotel...");
        await new Promise(resolve => setTimeout(resolve, 3000));
      }
    }

    console.log("\n🎉 Hotel data fetching completed!");

  } catch (error) {
    console.error("❌ Fatal error:", error.message);
  } finally {
    // Close database connections
    await closePool();
  }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n🛑 Received SIGINT, shutting down gracefully...');
  await closePool();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('\n🛑 Received SIGTERM, shutting down gracefully...');
  await closePool();
  process.exit(0);
});

// Run the main function
main().catch(console.error);
