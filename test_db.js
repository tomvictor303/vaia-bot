import 'dotenv/config';
import { closePool, executeQuery, testConnection } from './config/database.js';
import { MD_ALL_FIELDS, TABLE_NAMES } from './middleware/constants.js';
import { LogRunsService } from './services/log/logRunsService.js';
import { LogRunEventsService } from './services/log/logRunEventsService.js';
import { LogPagesService } from './services/log/logPagesService.js';
import { LogCategoriesService } from './services/log/logCategoriesService.js';
import { LogFailuresService } from './services/log/logFailuresService.js';

function namesFromFieldDefs(fieldDefs = []) {
  return fieldDefs
    .map((f) => f?.name)
    .filter((name) => typeof name === 'string' && name.length > 0);
}

async function getTableColumns(tableName) {
  const query = `
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = ?
  `;
  const rows = await executeQuery(query, [tableName]);
  return rows.map((r) => r.COLUMN_NAME);
}

async function checkTableFields({ serviceName, tableName, expectedFields }) {
  const columns = await getTableColumns(tableName);
  if (columns.length === 0) {
    return {
      ok: false,
      serviceName,
      tableName,
      missing: expectedFields,
      reason: 'table_not_found_or_no_columns',
    };
  }

  const columnSet = new Set(columns);
  const missing = expectedFields.filter((f) => !columnSet.has(f));

  return {
    ok: missing.length === 0,
    serviceName,
    tableName,
    missing,
    reason: '',
  };
}

async function main() {
  const connected = await testConnection();
  if (!connected) {
    process.exit(1);
  }

  const checks = [
    {
      serviceName: 'LogRunsService',
      tableName: LogRunsService.TABLE,
      expectedFields: Array.from(
        new Set([
          ...namesFromFieldDefs(LogRunsService.INSERTABLE_FIELDS),
          ...namesFromFieldDefs(LogRunsService.UPDATABLE_FIELDS),
          'id',
        ])
      ),
    },
    {
      serviceName: 'LogRunEventsService',
      tableName: LogRunEventsService.TABLE,
      expectedFields: Array.from(
        new Set([
          ...namesFromFieldDefs(LogRunEventsService.INSERTABLE_FIELDS),
          'id',
        ])
      ),
    },
    {
      serviceName: 'LogPagesService',
      tableName: LogPagesService.TABLE,
      expectedFields: Array.from(
        new Set([
          ...namesFromFieldDefs(LogPagesService.INSERTABLE_FIELDS),
          ...namesFromFieldDefs(LogPagesService.UPDATABLE_FIELDS),
          'id',
        ])
      ),
    },
    {
      serviceName: 'LogCategoriesService',
      tableName: LogCategoriesService.TABLE,
      expectedFields: Array.from(
        new Set([
          ...namesFromFieldDefs(LogCategoriesService.INSERTABLE_FIELDS),
          ...namesFromFieldDefs(LogCategoriesService.UPDATABLE_FIELDS),
          'id',
        ])
      ),
    },
    {
      serviceName: 'LogFailuresService',
      tableName: LogFailuresService.TABLE,
      expectedFields: Array.from(
        new Set([
          ...namesFromFieldDefs(LogFailuresService.INSERTABLE_FIELDS),
          'id',
        ])
      ),
    },
    // MarketDataService-related schema fields referenced by service logic.
    {
      serviceName: 'MarketDataService.market_data',
      tableName: TABLE_NAMES.MARKET_DATA_TABLE,
      expectedFields: Array.from(
        new Set([
          'id',
          'hotel_uuid',
          'is_deleted',
          'updated_at',
          ...MD_ALL_FIELDS.map((f) => f.name),
        ])
      ),
    },
    {
      serviceName: 'MarketDataService.market_data_debug1',
      tableName: TABLE_NAMES.MARKET_DATA_DEBUG1_TABLE,
      expectedFields: Array.from(
        new Set([
          'id',
          'hotel_uuid',
          'is_deleted',
          'updated_at',
          ...MD_ALL_FIELDS.map((f) => f.name),
        ])
      ),
    },
    {
      serviceName: 'MarketDataService.market_data_debug2',
      tableName: TABLE_NAMES.MARKET_DATA_DEBUG2_TABLE,
      expectedFields: Array.from(
        new Set([
          'id',
          'hotel_uuid',
          'is_deleted',
          'updated_at',
          ...MD_ALL_FIELDS.map((f) => f.name),
        ])
      ),
    },
  ];

  let hasErrors = false;

  for (const check of checks) {
    const result = await checkTableFields(check);
    if (result.ok) {
      console.log(`✅ ${result.serviceName}: all referenced fields exist in "${result.tableName}"`);
      continue;
    }

    hasErrors = true;
    console.error(`❌ ${result.serviceName}: schema mismatch in "${result.tableName}"`);
    if (result.reason) {
      console.error(`   reason: ${result.reason}`);
    }
    for (const field of result.missing) {
      console.error(`   missing field: ${field}`);
    }
  }

  if (hasErrors) {
    process.exitCode = 1;
  } else {
    console.log('🎉 DB schema check passed');
  }
}

main()
  .catch((error) => {
    console.error('❌ test_db failed:', error?.message || error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closePool();
  });
