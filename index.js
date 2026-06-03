require("dotenv").config();
const fs = require("fs");
const path = require("path");
const { MongoClient } = require("mongodb");
const cron = require("node-cron");
const { XeroClient } = require("xero-node");

// Configuration
const TOKEN_PATH = process.env.TOKEN_PATH || "/Users/adam/AGI/claude-task-automation/storage/xero_token.json";
const MONGODB_URI = process.env.MONGODB_URI || "mongodb://localhost:27017/";
const DB_NAME = process.env.MONGODB_DB_NAME || "xero_data";
const SYNC_SCHEDULE = process.env.SYNC_SCHEDULE || "0 */4 * * *";
const SYNC_STATE_PATH = process.env.SYNC_STATE_PATH || path.join(__dirname, "sync_state.json");
const FULL_SYNC = process.env.FULL_SYNC === 'true';
const HISTORY_YEARS = parseInt(process.env.HISTORY_YEARS || '10');

// Rate limiting configuration
const RATE_LIMIT_CONFIG = {
  DEFAULT_DELAY_MS: parseInt(process.env.DEFAULT_DELAY_MS || '1000'),
  MAX_RETRIES: parseInt(process.env.MAX_RETRIES || '10'),
  MAX_BACKOFF_MS: parseInt(process.env.MAX_BACKOFF_MS || '300000'), // 5 minutes
  BACKOFF_MULTIPLIER: parseInt(process.env.BACKOFF_MULTIPLIER || '2'),
  JITTER_MAX_MS: parseInt(process.env.JITTER_MAX_MS || '1000'),
};

// Default page size for API calls
const DEFAULT_PAGE_SIZE = parseInt(process.env.DEFAULT_PAGE_SIZE || '100');

// Xero scopes
const defaultScopes = `offline_access 
  accounting.transactions accounting.transactions.read 
  accounting.reports.read accounting.reports.tenninetynine.read 
  accounting.budgets.read accounting.journals.read 
  accounting.settings accounting.settings.read 
  accounting.contacts accounting.contacts.read 
  accounting.attachments accounting.attachments.read 
  assets assets.read 
  files files.read 
  payroll.employees payroll.employees.read 
  payroll.payruns payroll.payruns.read 
  payroll.payslip payroll.payslip.read 
  payroll.settings payroll.settings.read 
  payroll.timesheets payroll.timesheets.read 
  projects projects.read`;

const scopes = (process.env.XERO_SCOPES || defaultScopes)
  .split(/\s+/)
  .filter(Boolean);

// Initialize Xero client
// Note: We don't need clientId/clientSecret since another process handles OAuth
// We'll load the token directly from the file
const xero = new XeroClient({
  clientId: "placeholder", // Required by constructor but not used
  clientSecret: "placeholder", // Required by constructor but not used
  redirectUris: ["http://localhost:8080/callback"],
  scopes,
  state: "syncState",
});

// Global variables
let dbClient;
let db;
let tokenSet;
let syncState = {};

// Load sync state
function loadSyncState() {
  try {
    if (fs.existsSync(SYNC_STATE_PATH) && !FULL_SYNC) {
      syncState = JSON.parse(fs.readFileSync(SYNC_STATE_PATH, 'utf8'));
      console.log("📂 Loaded sync state from", SYNC_STATE_PATH);
    } else {
      syncState = {};
      console.log("🆕 Starting fresh sync state");
    }
  } catch (error) {
    console.error("Failed to load sync state:", error.message);
    syncState = {};
  }
}

// Save sync state
function saveSyncState() {
  try {
    fs.writeFileSync(SYNC_STATE_PATH, JSON.stringify(syncState, null, 2));
  } catch (error) {
    console.error("Failed to save sync state:", error.message);
  }
}

// Helper function to delay execution with jitter
const delay = (ms) => {
  const jitter = Math.random() * RATE_LIMIT_CONFIG.JITTER_MAX_MS;
  return new Promise((resolve) => setTimeout(resolve, ms + jitter));
};

// Calculate exponential backoff delay
function calculateBackoffDelay(retryCount, retryAfterHeader) {
  if (retryAfterHeader) {
    return parseInt(retryAfterHeader) * 1000;
  }
  
  const exponentialDelay = Math.min(
    RATE_LIMIT_CONFIG.DEFAULT_DELAY_MS * Math.pow(RATE_LIMIT_CONFIG.BACKOFF_MULTIPLIER, retryCount),
    RATE_LIMIT_CONFIG.MAX_BACKOFF_MS
  );
  
  return exponentialDelay;
}

// Execute API call with rate limiting and retry logic
async function executeWithRateLimiting(apiCall, context = '') {
  let retryCount = 0;
  
  while (retryCount <= RATE_LIMIT_CONFIG.MAX_RETRIES) {
    try {
      // Add delay before API call
      if (retryCount === 0) {
        await delay(RATE_LIMIT_CONFIG.DEFAULT_DELAY_MS);
      }
      
      return await apiCall();
    } catch (error) {
      const status = error.response?.status;
      const headers = error.response?.headers || {};
      
      // Handle rate limiting (429)
      if (status === 429 || (error.response?.statusText || '').includes('Too Many Requests')) {
        if (retryCount >= RATE_LIMIT_CONFIG.MAX_RETRIES) {
          console.error(`❌ Rate limit exceeded after ${RATE_LIMIT_CONFIG.MAX_RETRIES} retries. ${context}`);
          throw error;
        }
        
        const backoffDelay = calculateBackoffDelay(retryCount, headers["retry-after"]);
        console.warn(`⏳ Rate limited. Waiting ${(backoffDelay/1000).toFixed(1)}s before retry ${retryCount + 1}/${RATE_LIMIT_CONFIG.MAX_RETRIES}. ${context}`);
        
        await delay(backoffDelay);
        retryCount++;
        continue;
      }
      
      // Handle server errors (5xx) and network errors
      if (status >= 500 || status === 408 || !status) {
        if (retryCount < RATE_LIMIT_CONFIG.MAX_RETRIES) {
          const backoffDelay = calculateBackoffDelay(retryCount);
          console.warn(`⚠️  Retryable error (${status || 'network'}). Waiting ${(backoffDelay/1000).toFixed(1)}s before retry ${retryCount + 1}/${RATE_LIMIT_CONFIG.MAX_RETRIES}. ${context}`);
          
          await delay(backoffDelay);
          retryCount++;
          continue;
        }
      }
      
      // Non-retryable error
      throw error;
    }
  }
}

// Connect to MongoDB
async function connectToMongoDB() {
  try {
    if (!dbClient) {
      console.log("🔗 Connecting to MongoDB...");
      dbClient = new MongoClient(MONGODB_URI);
      await dbClient.connect();
      db = dbClient.db(DB_NAME);
      console.log("✅ Connected to MongoDB successfully");
    }
    return db;
  } catch (error) {
    console.error("❌ Failed to connect to MongoDB:", error.message);
    throw error;
  }
}

// Load Xero token
async function loadXeroToken() {
  try {
    if (!fs.existsSync(TOKEN_PATH)) {
      throw new Error(`Token file not found at ${TOKEN_PATH}`);
    }

    tokenSet = JSON.parse(fs.readFileSync(TOKEN_PATH, "utf8"));
    xero.setTokenSet(tokenSet);
    console.log("🔑 Loaded Xero token successfully");
    return true;
  } catch (error) {
    console.error("❌ Failed to load Xero token:", error.message);
    return false;
  }
}

// Initialize Xero
async function initializeXero() {
  try {
    await xero.initialize();
    console.log("✅ Xero client initialized");
    
    if (!(await loadXeroToken())) {
      console.error("❌ Failed to load Xero token. Sync cannot proceed.");
      return false;
    }
    
    return true;
  } catch (error) {
    console.error("❌ Failed to initialize Xero client:", error.message);
    return false;
  }
}

// Generic sync function with pagination
async function syncEntity(config) {
  const {
    name,
    collection,
    apiCall,
    idField,
    processItems = (items) => items,
    supportsModifiedSince = true,
    supportsPagination = true,
    pageSize = DEFAULT_PAGE_SIZE,
  } = config;

  try {
    await loadXeroToken();
    const tenants = await xero.updateTenants();
    const firstTenant = tenants[0];

    // Determine sync mode
    const lastSync = syncState[name]?.lastSync;
    const isIncremental = !FULL_SYNC && lastSync && supportsModifiedSince;
    const modifiedSince = isIncremental ? new Date(lastSync) : null;

    console.log(`\n📊 Syncing ${name}`);
    console.log(`   Mode: ${isIncremental ? 'INCREMENTAL' : 'FULL'}`);
    if (modifiedSince) {
      console.log(`   Since: ${modifiedSince.toISOString()}`);
    }

    let allItems = [];
    let page = 1;
    let hasMore = true;
    let totalPages = 0;
    let seenIds = new Set();
    let duplicateCount = 0;
    const MAX_PAGES = 100; // Safety limit

    while (hasMore && page <= MAX_PAGES) {
      try {
        const response = await executeWithRateLimiting(
          () => apiCall(firstTenant.tenantId, modifiedSince, page, pageSize),
          `${name} page ${page}`
        );

        const items = processItems(response.body);
        
        // Check for duplicates
        let newItemsCount = 0;
        const newItems = [];
        for (const item of items) {
          const itemId = item[idField];
          if (!seenIds.has(itemId)) {
            seenIds.add(itemId);
            newItems.push(item);
            newItemsCount++;
          } else {
            duplicateCount++;
          }
        }
        
        if (newItemsCount > 0) {
          allItems = allItems.concat(newItems);
          console.log(`   📄 Page ${page}: ${items.length} items (${newItemsCount} new, total: ${allItems.length})`);
        } else if (items.length > 0) {
          console.log(`   ⚠️  Page ${page}: All ${items.length} items were duplicates, stopping pagination`);
          hasMore = false;
          break;
        }

        totalPages++;
        
        if (!supportsPagination || items.length < pageSize || newItemsCount === 0) {
          hasMore = false;
        } else {
          page++;
        }
      } catch (error) {
        console.error(`   ❌ Error on page ${page}:`, error.message);
        if (page === 1) throw error; // Fail if first page fails
        hasMore = false; // Otherwise continue with what we have
      }
    }
    
    if (duplicateCount > 0) {
      console.log(`   ℹ️  Filtered out ${duplicateCount} duplicate items from API response`);
    }
    
    if (page > MAX_PAGES) {
      console.log(`   ⚠️  Reached maximum page limit (${MAX_PAGES}), stopping to prevent infinite loop`);
    }

    if (allItems.length === 0) {
      console.log(`   ✅ No items to sync`);
      syncState[name] = { lastSync: Date.now(), count: 0 };
      saveSyncState();
      return 0;
    }

    // Bulk upsert to MongoDB
    const db = await connectToMongoDB();
    const dbCollection = db.collection(collection);
    
    const CHUNK_SIZE = 500;
    let totalUpserted = 0;
    let totalModified = 0;

    for (let i = 0; i < allItems.length; i += CHUNK_SIZE) {
      const chunk = allItems.slice(i, i + CHUNK_SIZE);
      const bulkOps = chunk.map(item => ({
        updateOne: {
          filter: { [idField]: item[idField] },
          update: { 
            $set: { 
              ...item, 
              _syncedAt: new Date(),
              _lastModified: item.updatedDateUTC || item.dateUTC || new Date()
            } 
          },
          upsert: true
        }
      }));

      const result = await dbCollection.bulkWrite(bulkOps, { ordered: false });
      totalUpserted += result.upsertedCount;
      totalModified += result.modifiedCount;
      
      // Debug logging for chunks
      if (result.upsertedCount > 0 || result.modifiedCount > 0) {
        console.log(`   💾 Chunk ${Math.floor(i / CHUNK_SIZE) + 1}: ${result.upsertedCount} new, ${result.modifiedCount} updated`);
      }
    }

    // Update sync state
    syncState[name] = {
      lastSync: Date.now(),
      count: allItems.length,
      upserted: totalUpserted,
      modified: totalModified,
      pages: totalPages
    };
    saveSyncState();

    console.log(`   ✅ Complete: ${allItems.length} items (${totalUpserted} new, ${totalModified} updated)`);
    return allItems.length;

  } catch (error) {
    console.error(`   ❌ Failed to sync ${name}:`, error.message);
    return 0;
  }
}

// Define all entities to sync
const syncConfigs = [
  {
    name: 'accounts',
    collection: 'accounts',
    idField: 'accountID',
    supportsModifiedSince: true,
    supportsPagination: false,
    apiCall: (tenantId, modifiedSince) => 
      xero.accountingApi.getAccounts(tenantId, modifiedSince),
    processItems: (body) => body.accounts || []
  },
  {
    name: 'contacts',
    collection: 'contacts',
    idField: 'contactID',
    apiCall: (tenantId, modifiedSince, page, pageSize) => {
      // Contacts API uses different parameter order
      return xero.accountingApi.getContacts(
        tenantId,     // tenantId
        modifiedSince, // ifModifiedSince
        undefined,    // where
        undefined,    // order
        undefined,    // IDs
        page,         // page
        undefined,    // includeArchived
        undefined,    // summaryOnly
        undefined     // searchTerm
      );
    },
    processItems: (body) => body.contacts || []
  },
  {
    name: 'invoices',
    collection: 'invoices',
    idField: 'invoiceID',
    apiCall: (tenantId, modifiedSince, page, pageSize) => {
      const where = FULL_SYNC && !modifiedSince 
        ? `Date >= DateTime(${new Date().getFullYear() - HISTORY_YEARS}, 1, 1)` 
        : undefined;
      return xero.accountingApi.getInvoices(
        tenantId, modifiedSince, where, undefined, undefined, undefined, 
        undefined, undefined, page, undefined, undefined, 4, false, pageSize
      );
    },
    processItems: (body) => body.invoices || []
  },
  {
    name: 'bankTransactions',
    collection: 'bank_transactions',
    idField: 'bankTransactionID',
    apiCall: (tenantId, modifiedSince, page, pageSize) =>
      xero.accountingApi.getBankTransactions(
        tenantId, modifiedSince, undefined, undefined, page, 4, pageSize
      ),
    processItems: (body) => body.bankTransactions || []
  },
  {
    name: 'creditNotes',
    collection: 'credit_notes',
    idField: 'creditNoteID',
    apiCall: (tenantId, modifiedSince, page, pageSize) =>
      xero.accountingApi.getCreditNotes(
        tenantId, modifiedSince, undefined, undefined, page, 4, undefined,
        undefined, undefined, undefined, undefined, pageSize
      ),
    processItems: (body) => body.creditNotes || []
  },
  {
    name: 'purchaseOrders',
    collection: 'purchase_orders',
    idField: 'purchaseOrderID',
    apiCall: (tenantId, modifiedSince, page, pageSize) => {
      const dateFrom = FULL_SYNC && !modifiedSince
        ? new Date(new Date().getFullYear() - HISTORY_YEARS, 0, 1).toISOString()
        : undefined;
      return xero.accountingApi.getPurchaseOrders(
        tenantId, modifiedSince, undefined, dateFrom, undefined, undefined, page, pageSize
      );
    },
    processItems: (body) => body.purchaseOrders || []
  },
  {
    name: 'payments',
    collection: 'payments',
    idField: 'paymentID',
    apiCall: (tenantId, modifiedSince, page, pageSize) =>
      xero.accountingApi.getPayments(
        tenantId, modifiedSince, undefined, undefined, page, undefined,
        undefined, undefined, undefined, undefined, pageSize
      ),
    processItems: (body) => body.payments || []
  },
  {
    name: 'items',
    collection: 'items',
    idField: 'itemID',
    supportsPagination: false,
    apiCall: (tenantId, modifiedSince) =>
      xero.accountingApi.getItems(tenantId, modifiedSince),
    processItems: (body) => body.items || []
  },
  {
    name: 'taxRates',
    collection: 'tax_rates',
    idField: 'name',
    supportsModifiedSince: false,
    supportsPagination: false,
    apiCall: (tenantId) =>
      xero.accountingApi.getTaxRates(tenantId),
    processItems: (body) => body.taxRates || []
  },
  {
    name: 'trackingCategories',
    collection: 'tracking_categories',
    idField: 'trackingCategoryID',
    supportsModifiedSince: false,
    supportsPagination: false,
    apiCall: (tenantId) =>
      xero.accountingApi.getTrackingCategories(tenantId),
    processItems: (body) => body.trackingCategories || []
  },
  {
    name: 'bankTransfers',
    collection: 'bank_transfers',
    idField: 'bankTransferID',
    apiCall: (tenantId, modifiedSince, page, pageSize) =>
      xero.accountingApi.getBankTransfers(
        tenantId, modifiedSince, undefined, undefined, page, pageSize
      ),
    processItems: (body) => body.bankTransfers || []
  },
  {
    name: 'manualJournals',
    collection: 'manual_journals',
    idField: 'manualJournalID',
    apiCall: (tenantId, modifiedSince, page, pageSize) =>
      xero.accountingApi.getManualJournals(
        tenantId, modifiedSince, undefined, undefined, page, undefined, undefined, pageSize
      ),
    processItems: (body) => body.manualJournals || []
  },
  {
    name: 'journals',
    collection: 'journals',
    idField: 'journalID',
    apiCall: (tenantId, modifiedSince, page, pageSize) => {
      const offset = (page - 1) * pageSize;
      return xero.accountingApi.getJournals(
        tenantId, modifiedSince, offset, undefined
      );
    },
    processItems: (body) => body.journals || []
  },
  {
    name: 'budgets',
    collection: 'budgets',
    idField: 'budgetID',
    supportsModifiedSince: false,
    supportsPagination: false,
    apiCall: (tenantId) =>
      xero.accountingApi.getBudgets(tenantId),
    processItems: (body) => body.budgets || []
  },
  {
    name: 'repeatingInvoices',
    collection: 'repeating_invoices',
    idField: 'repeatingInvoiceID',
    supportsModifiedSince: false,
    supportsPagination: false,
    apiCall: (tenantId) =>
      xero.accountingApi.getRepeatingInvoices(tenantId),
    processItems: (body) => body.repeatingInvoices || []
  },
  {
    name: 'organisations',
    collection: 'organisations',
    idField: 'organisationID',
    supportsModifiedSince: false,
    supportsPagination: false,
    apiCall: (tenantId) =>
      xero.accountingApi.getOrganisations(tenantId),
    processItems: (body) => body.organisations || []
  },
  // Additional entities for complete Xero replication
  {
    name: 'receipts',
    collection: 'receipts',
    idField: 'receiptID',
    apiCall: (tenantId, modifiedSince, page, pageSize) =>
      xero.accountingApi.getReceipts(
        tenantId, modifiedSince, undefined, undefined, 4, page, pageSize
      ),
    processItems: (body) => body.receipts || []
  },
  {
    name: 'expenseClaims',
    collection: 'expense_claims',
    idField: 'expenseClaimID',
    apiCall: (tenantId, modifiedSince, page) =>
      xero.accountingApi.getExpenseClaims(
        tenantId, modifiedSince, undefined, undefined, page
      ),
    processItems: (body) => body.expenseClaims || []
  },
  {
    name: 'currencies',
    collection: 'currencies',
    idField: 'code',
    supportsModifiedSince: false,
    supportsPagination: false,
    apiCall: (tenantId) =>
      xero.accountingApi.getCurrencies(tenantId),
    processItems: (body) => body.currencies || []
  },
  {
    name: 'employees',
    collection: 'employees',
    idField: 'employeeID',
    apiCall: (tenantId, modifiedSince, page) =>
      xero.accountingApi.getEmployees(
        tenantId, modifiedSince, undefined, undefined, page
      ),
    processItems: (body) => body.employees || []
  },
  {
    name: 'users',
    collection: 'users',
    idField: 'userID',
    supportsModifiedSince: false,
    supportsPagination: false,
    apiCall: (tenantId) =>
      xero.accountingApi.getUsers(tenantId),
    processItems: (body) => body.users || []
  },
  {
    name: 'linkedTransactions',
    collection: 'linked_transactions',
    idField: 'linkedTransactionID',
    apiCall: (tenantId, modifiedSince, page) =>
      xero.accountingApi.getLinkedTransactions(
        tenantId, page, undefined, undefined, undefined, undefined,
        undefined, undefined, undefined, modifiedSince
      ),
    processItems: (body) => body.linkedTransactions || []
  },
  {
    name: 'prepayments',
    collection: 'prepayments',
    idField: 'prepaymentID',
    apiCall: (tenantId, modifiedSince, page) =>
      xero.accountingApi.getPrepayments(
        tenantId, modifiedSince, undefined, undefined, page, 4
      ),
    processItems: (body) => body.prepayments || []
  },
  {
    name: 'overpayments',
    collection: 'overpayments',
    idField: 'overpaymentID',
    apiCall: (tenantId, modifiedSince, page) =>
      xero.accountingApi.getOverpayments(
        tenantId, modifiedSince, undefined, undefined, page, 4
      ),
    processItems: (body) => body.overpayments || []
  },
  {
    name: 'contactGroups',
    collection: 'contact_groups',
    idField: 'contactGroupID',
    supportsModifiedSince: false,
    apiCall: (tenantId, _, page) =>
      xero.accountingApi.getContactGroups(tenantId, undefined, undefined, page),
    processItems: (body) => body.contactGroups || []
  },
  {
    name: 'brandingThemes',
    collection: 'branding_themes',
    idField: 'brandingThemeID',
    supportsModifiedSince: false,
    supportsPagination: false,
    apiCall: (tenantId) =>
      xero.accountingApi.getBrandingThemes(tenantId),
    processItems: (body) => body.brandingThemes || []
  },
  {
    name: 'quotes',
    collection: 'quotes',
    idField: 'quoteID',
    apiCall: (tenantId, modifiedSince, page) =>
      xero.accountingApi.getQuotes(
        tenantId, modifiedSince, undefined, undefined, undefined,
        undefined, undefined, undefined, page, undefined
      ),
    processItems: (body) => body.quotes || []
  },
  {
    name: 'batchPayments',
    collection: 'batch_payments',
    idField: 'batchPaymentID',
    apiCall: (tenantId, modifiedSince, page) =>
      xero.accountingApi.getBatchPayments(
        tenantId, modifiedSince, undefined, undefined, page
      ),
    processItems: (body) => body.batchPayments || []
  }
];

// Run full sync
async function runSync() {
  const startTime = Date.now();
  
  console.log("\n" + "=".repeat(50));
  console.log("🚀 XERO TO MONGODB SYNC");
  console.log("=".repeat(50));
  console.log(`📅 Time: ${new Date().toISOString()}`);
  console.log(`🔄 Mode: ${FULL_SYNC ? 'FULL SYNC' : 'INCREMENTAL'}`);
  console.log(`📊 History: ${HISTORY_YEARS} years`);
  console.log(`⏱️  Rate limit: ${RATE_LIMIT_CONFIG.DEFAULT_DELAY_MS}ms delay`);
  console.log("=".repeat(50));

  // Load sync state
  loadSyncState();

  if (!(await initializeXero())) {
    console.error("❌ Failed to initialize Xero. Aborting sync.");
    return false;
  }

  const results = {
    success: [],
    failed: [],
    totalItems: 0,
    duration: 0
  };

  try {
    await connectToMongoDB();

    // Sync all entities
    for (const config of syncConfigs) {
      const count = await syncEntity(config);
      
      if (count >= 0) {
        results.success.push({ name: config.name, count });
        results.totalItems += count;
      } else {
        results.failed.push(config.name);
      }
      
      // Small delay between entity types
      await delay(RATE_LIMIT_CONFIG.DEFAULT_DELAY_MS * 2);
    }

    results.duration = (Date.now() - startTime) / 1000;

    // Print summary
    console.log("\n" + "=".repeat(50));
    console.log("📈 SYNC SUMMARY");
    console.log("=".repeat(50));
    console.log(`⏱️  Duration: ${results.duration.toFixed(1)} seconds`);
    console.log(`📊 Total items: ${results.totalItems.toLocaleString()}`);
    console.log(`✅ Successful: ${results.success.length}/${syncConfigs.length}`);
    
    if (results.failed.length > 0) {
      console.log(`❌ Failed: ${results.failed.join(', ')}`);
    }
    
    console.log("=".repeat(50) + "\n");

    return results.failed.length === 0;

  } catch (error) {
    console.error("❌ Sync failed:", error.message);
    return false;
  }
}

// Schedule sync
function scheduleSync() {
  console.log(`⏰ Scheduling sync with pattern: ${SYNC_SCHEDULE}`);
  
  cron.schedule(SYNC_SCHEDULE, async () => {
    console.log(`\n🔄 Running scheduled sync at ${new Date().toISOString()}`);
    await runSync();
  });
}

// Main function
async function main() {
  console.log("\n🚀 Starting Xero Sync Service");
  console.log(`📁 Token: ${path.resolve(TOKEN_PATH)}`);
  console.log(`💾 State: ${path.resolve(SYNC_STATE_PATH)}`);
  console.log(`🔄 Schedule: ${SYNC_SCHEDULE}\n`);

  // Run initial sync
  await runSync();

  // Schedule recurring syncs
  if (process.env.RUN_ONCE !== 'true') {
    scheduleSync();
  } else {
    console.log("✅ RUN_ONCE mode - exiting after sync");
    process.exit(0);
  }
}

// Start the service
main().catch((err) => {
  console.error("❌ Fatal error:", err);
  process.exit(1);
});

// Graceful shutdown
process.on("SIGINT", async () => {
  console.log("\n🛑 Shutting down...");
  if (dbClient) {
    await dbClient.close();
    console.log("✅ MongoDB connection closed");
  }
  process.exit(0);
});

process.on('uncaughtException', (error) => {
  console.error('❌ Uncaught Exception:', error);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ Unhandled Rejection at:', promise, 'reason:', reason);
  process.exit(1);
});