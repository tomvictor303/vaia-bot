import 'dotenv/config';
import { testConnection, closePool } from './config/database.js';
import { HotelService } from './services/hotelService.js';
import { scrapeHotel } from './controllers/scrapeController.js';
import { aggregateScrapedData } from './controllers/aggregateScrapedController.js';
async function main() {
  console.log("🚀 Starting Hotel Data Fetcher...");

  const isUnitTest = String(process.env.UNIT_TEST || '').toLowerCase() === 'true';
  const unitTestModule = String(process.env.UNIT_TEST_MODULE || '').toLowerCase();
  const shouldRunScrape = !isUnitTest || unitTestModule === 'scrape';
  const shouldRunAggregate = !isUnitTest || unitTestModule === 'ai';

  if (isUnitTest) {
    if (!unitTestModule) {
      throw new Error('UNIT_TEST mode requires UNIT_TEST_MODULE to be set (scrape|ai)');
    }
    console.log(`🧪 UNIT_TEST mode enabled (module: ${unitTestModule})`);
  }
  
  // Test database connection
  const isConnected = await testConnection();
  if (!isConnected) {
    console.error("❌ Cannot proceed without database connection");
    process.exit(1);
  }

  // LLM configuration (API key omitted)
  console.log('🤖 LLM config:', {
    LLM_API_BASE_URL: process.env.LLM_API_BASE_URL || '(not set)',
    LLM_MODEL_VERSION: process.env.LLM_MODEL_VERSION || '(not set)',
    LLM_PROMPT_VERSION: process.env.LLM_PROMPT_VERSION || '(not set)',
    LLM_API_KEY: process.env.LLM_API_KEY ? '(set)' : '(not set)',
  });

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

      if (hotel.hotel_url) {
        // BEGIN PROCESS_SINGLE_HOTEL
        // BEGIN SCRAPE_HOTEL
        let scrapedSuccess = false;
        if (shouldRunScrape) {
          // BEGIN SCRAPE_HOTEL_BODY
          try {
            await scrapeHotel(hotel.hotel_url, hotel.hotel_uuid, hotel.name);
            scrapedSuccess = true;
          } catch (error) {
            console.error(`❌ Error scraping ${hotel.name}:`, error.message);
            console.log("⏭️  Continuing with next hotel...");
          }
          // END SCRAPE_HOTEL_BODY
        } else {
          scrapedSuccess = true; // Set true for next modules for unit test
          console.log('ℹ️  UNIT_TEST: skipping scrape step');
        }
        // END SCRAPE_HOTEL

        // BEGIN AGGREGATE_SCRAPED_HOTEL_DATA
        if (shouldRunAggregate && scrapedSuccess) {
          // BEGIN AGGREGATE_SCRAPED_HOTEL_DATA_BODY
          await aggregateScrapedData(hotel.hotel_uuid, hotel.name);
          // END AGGREGATE_SCRAPED_HOTEL_DATA_BODY
        }
        // END AGGREGATE_SCRAPED_HOTEL_DATA 
        // END PROCESS_SINGLE_HOTEL
      } else {
        console.log(`⚠️  No hotel_url found for ${hotel.name}, skipping scraping`);
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
