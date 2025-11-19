// Unified Queue Consumer Worker for BuzzLine
// Handles: Contact Import, Campaign Sending, Contact Deletion

// KV service with rate limit tracking
class KVService {
  constructor(env) {
    this.main = env.BUZZLINE_MAIN;
    this.cache = env.BUZZLINE_CACHE;
    this.analytics = env.BUZZLINE_ANALYTICS;
    this.operationCount = 0;
    this.maxOperations = 950; // Leave buffer under 1000 limit
  }
  
  checkRateLimit() {
    if (this.operationCount >= this.maxOperations) {
      throw new Error(`KV rate limit reached: ${this.operationCount}/${this.maxOperations} operations`);
    }
  }
  
  incrementOperationCount() {
    this.operationCount++;
    if (this.operationCount % 100 === 0) {
      console.log(`📊 KV Operations: ${this.operationCount}/${this.maxOperations}`);
    }
  }

  async getCache(key) {
    this.checkRateLimit();
    try {
      this.incrementOperationCount();
      const result = await this.cache.get(key);
      return result ? JSON.parse(result) : null;
    } catch (error) {
      console.error('KV get error:', error);
      return null;
    }
  }

  async setCache(key, value, ttl = 3600) {
    this.checkRateLimit();
    try {
      this.incrementOperationCount();
      await this.cache.put(key, JSON.stringify(value), { expirationTtl: ttl });
    } catch (error) {
      console.error('KV set error:', error);
    }
  }

  async deleteCache(key) {
    this.checkRateLimit();
    try {
      this.incrementOperationCount();
      await this.cache.delete(key);
    } catch (error) {
      console.error('KV cache delete error:', error);
    }
  }

  async get(key) {
    this.checkRateLimit();
    try {
      this.incrementOperationCount();
      const result = await this.main.get(key);
      return result ? JSON.parse(result) : null;
    } catch (error) {
      console.error('KV main get error:', error);
      return null;
    }
  }

  async put(key, value) {
    this.checkRateLimit();
    try {
      this.incrementOperationCount();
      await this.main.put(key, JSON.stringify(value));
    } catch (error) {
      console.error('KV main put error:', error);
    }
  }

  async delete(key) {
    this.checkRateLimit();
    try {
      this.incrementOperationCount();
      await this.main.delete(key);
    } catch (error) {
      console.error('KV main delete error:', error);
      throw error;
    }
  }

  async listMain(options = {}) {
    this.checkRateLimit();
    try {
      this.incrementOperationCount();
      return await this.main.list(options);
    } catch (error) {
      console.error('KV main list error:', error);
      throw error;
    }
  }
}

// Message types for unified queue processing
const MESSAGE_TYPES = {
  CONTACT_BATCH: 'contact_batch',
  CAMPAIGN_SEND: 'campaign_send',
  CONTACT_DELETE: 'contact_delete',
  OPT_OUT_BATCH: 'opt_out_batch',
  BULK_DELETE_ORG_DATA: 'bulk_delete_org_data',
  METADATA_REBUILD: 'metadata_rebuild',
  SEGMENT_BUILD: 'segment_build'
};

// Mailgun public docs cap default sending to roughly 300 requests per minute per domain.
// https://documentation.mailgun.com/en/latest/user_manual.html#per-domain-rate-limits
const CAMPAIGN_SEND_BATCH_SIZE = 25;
const CAMPAIGN_BATCH_DELAY_MS = 500;
const MAILGUN_MAX_EMAILS_PER_MINUTE = 300;
const MAILGUN_RATE_LIMIT_WINDOW_MS = 60_000;

// Contact metadata management
class ContactMetadataService {
  constructor(kvService) {
    this.kv = kvService;
  }

  createEmptyMetadata() {
    return {
      totalContacts: 0,
      contactsWithEmail: 0,
      contactsWithPhone: 0,
      contactsWithBoth: 0,
      subscribedContacts: 0,
      optedOutContacts: 0,
      lastUpdated: new Date().toISOString(),
      version: 0
    };
  }
  
  // Get or initialize contact metadata for an organization
  async getContactMetadata(orgId) {
    const metaKey = `org:${orgId}:contact_metadata`;
    const metadata = await this.kv.get(metaKey);
    
    if (metadata) {
      return metadata;
    }
    
    // Initialize metadata if it doesn't exist
    const initialMetadata = this.createEmptyMetadata();
    initialMetadata.version = 1;
    
    await this.kv.put(metaKey, initialMetadata);
    return initialMetadata;
  }
  
  // Update contact metadata when contacts are added/modified/deleted
  async updateContactMetadata(orgId, operations) {
    const metaKey = `org:${orgId}:contact_metadata`;
    const metadata = await this.getContactMetadata(orgId);
    
    // Apply operations to metadata
    for (const op of operations) {
      switch (op.type) {
        case 'add':
          this.applyAddOperation(metadata, op.contact);
          break;
        case 'update':
          this.applyUpdateOperation(metadata, op.oldContact, op.newContact);
          break;
        case 'delete':
          this.applyDeleteOperation(metadata, op.contact);
          break;
        case 'bulk_add':
          this.applyBulkAddOperation(metadata, op.contacts);
          break;
        case 'bulk_delete':
          this.applyBulkDeleteOperation(metadata, op.count);
          break;
      }
    }
    
    metadata.lastUpdated = new Date().toISOString();
    metadata.version += 1;
    
    await this.kv.put(metaKey, metadata);
    return metadata;
  }
  
  applyAddOperation(metadata, contact) {
    metadata.totalContacts++;
    
    const hasEmail = contact.email && contact.email.trim();
    const hasPhone = contact.phone && contact.phone.trim();
    
    if (hasEmail) metadata.contactsWithEmail++;
    if (hasPhone) metadata.contactsWithPhone++;
    if (hasEmail && hasPhone) metadata.contactsWithBoth++;
    
    if (!contact.optedOut) {
      metadata.subscribedContacts++;
    } else {
      metadata.optedOutContacts++;
    }
  }
  
  applyUpdateOperation(metadata, oldContact, newContact) {
    const oldHasEmail = oldContact.email && oldContact.email.trim();
    const oldHasPhone = oldContact.phone && oldContact.phone.trim();
    const newHasEmail = newContact.email && newContact.email.trim();
    const newHasPhone = newContact.phone && newContact.phone.trim();
    
    // Update email counts
    if (oldHasEmail && !newHasEmail) metadata.contactsWithEmail--;
    if (!oldHasEmail && newHasEmail) metadata.contactsWithEmail++;
    
    // Update phone counts
    if (oldHasPhone && !newHasPhone) metadata.contactsWithPhone--;
    if (!oldHasPhone && newHasPhone) metadata.contactsWithPhone++;
    
    // Update both counts
    if (oldHasEmail && oldHasPhone && !(newHasEmail && newHasPhone)) {
      metadata.contactsWithBoth--;
    }
    if (!(oldHasEmail && oldHasPhone) && newHasEmail && newHasPhone) {
      metadata.contactsWithBoth++;
    }
    
    // Update subscription status
    if (!oldContact.optedOut && newContact.optedOut) {
      metadata.subscribedContacts--;
      metadata.optedOutContacts++;
    }
    if (oldContact.optedOut && !newContact.optedOut) {
      metadata.subscribedContacts++;
      metadata.optedOutContacts--;
    }
  }
  
  applyDeleteOperation(metadata, contact) {
    metadata.totalContacts--;
    
    const hasEmail = contact.email && contact.email.trim();
    const hasPhone = contact.phone && contact.phone.trim();
    
    if (hasEmail) metadata.contactsWithEmail--;
    if (hasPhone) metadata.contactsWithPhone--;
    if (hasEmail && hasPhone) metadata.contactsWithBoth--;
    
    if (!contact.optedOut) {
      metadata.subscribedContacts--;
    } else {
      metadata.optedOutContacts--;
    }
  }
  
  applyBulkAddOperation(metadata, contacts) {
    for (const contact of contacts) {
      this.applyAddOperation(metadata, contact);
    }
  }
  
  applyBulkDeleteOperation(metadata, deleteCount) {
    // For bulk deletion, we reset to 0 since we're typically clearing all
    metadata.totalContacts = 0;
    metadata.contactsWithEmail = 0;
    metadata.contactsWithPhone = 0;
    metadata.contactsWithBoth = 0;
    metadata.subscribedContacts = 0;
    metadata.optedOutContacts = 0;
  }
  
  // Rebuild metadata from scratch by scanning all contacts
  async rebuildContactMetadata(orgId) {
    console.log('🔄 REBUILDING CONTACT METADATA for org:', orgId);

    const prefix = `org:${orgId}:contact:`;
    const metadata = this.createEmptyMetadata();

    let cursor = undefined;
    let batchNumber = 0;

    do {
      batchNumber++;
      // Use smaller batch size to avoid rate limits (Workers: 1000 reads/sec limit)
      const list = await this.kv.listMain({ prefix, limit: 500, cursor });

      console.log(`📊 Metadata rebuild batch ${batchNumber}: ${list.keys.length} contacts`);

      // Process contacts in smaller chunks to avoid rate limits
      const CHUNK_SIZE = 100;
      const allContacts = [];

      for (let i = 0; i < list.keys.length; i += CHUNK_SIZE) {
        const chunk = list.keys.slice(i, i + CHUNK_SIZE);
        const chunkPromises = chunk.map(async (key) => {
          try {
            return await this.kv.get(key.name);
          } catch (error) {
            console.error('Failed to parse contact:', error);
            return null;
          }
        });

        const chunkResults = await Promise.all(chunkPromises);
        allContacts.push(...chunkResults.filter(Boolean));

        // Small delay between chunks to avoid rate limits (100 reads per 100ms = 1000 reads/sec max)
        if (i + CHUNK_SIZE < list.keys.length) {
          await new Promise(resolve => setTimeout(resolve, 100));
        }
      }

      // Update metadata with this batch
      this.applyBulkAddOperation(metadata, allContacts);

      cursor = list.list_complete ? undefined : list.cursor;

      // Delay between batches to stay under rate limits
      if (cursor) {
        await new Promise(resolve => setTimeout(resolve, 200));
      }
    } while (cursor);

    // Save rebuilt metadata
    const metaKey = `org:${orgId}:contact_metadata`;
    await this.kv.put(metaKey, metadata);

    console.log('✅ METADATA REBUILT:', metadata);
    return metadata;
  }
}

// Simplified contact service for the worker
class ContactService {
  constructor(env) {
    this.kv = new KVService(env);
    this.metadataService = new ContactMetadataService(this.kv);
  }

  async findContactsByEmailsOrPhones(orgId, emailsAndPhones) {
    try {
      const kvPromises = [];
      const lookupMap = new Map();
      
      // Batch all KV reads into parallel operations
      emailsAndPhones.forEach(({ email, phone }, index) => {
        if (email) {
          const emailKey = `org:${orgId}:contact_by_email:${email.toLowerCase()}`;
          kvPromises.push(
            this.kv.get(emailKey).then(contact => ({ type: 'email', value: email, contact, index }))
          );
        }
        if (phone) {
          const phoneKey = `org:${orgId}:contact_by_phone:${phone}`;
          kvPromises.push(
            this.kv.get(phoneKey).then(contact => ({ type: 'phone', value: phone, contact, index }))
          );
        }
      });
      
      // Execute all KV operations in parallel
      const results = await Promise.allSettled(kvPromises);
      const contacts = [];
      
      results.forEach(result => {
        if (result.status === 'fulfilled' && result.value.contact) {
          const { type, value, contact } = result.value;
          contacts.push({ [type]: value, contact });
        }
      });
      
      return contacts;
    } catch (error) {
      console.error('Error finding contacts:', error);
      return [];
    }
  }

  async createContactsBulk(orgId, contacts) {
    const created = [];
    const errors = [];

    // PHASE 2 OPTIMIZATION: Collect ALL KV operations first, then execute in optimized chunks
    const allKvOperations = [];
    const contactMap = new Map(); // Track which operations belong to which contact

    // Build all KV operations upfront
    contacts.forEach(({ id, data }) => {
      const contact = {
        id,
        orgId,
        ...data,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString()
      };

      contactMap.set(id, contact);

      // Main contact record
      const contactKey = `org:${orgId}:contact:${id}`;
      allKvOperations.push({ type: 'put', key: contactKey, value: contact, contactId: id });

      // Email index
      if (contact.email) {
        const emailKey = `org:${orgId}:contact_by_email:${contact.email.toLowerCase()}`;
        allKvOperations.push({ type: 'put', key: emailKey, value: contact, contactId: id });
      }

      // Phone index
      if (contact.phone) {
        const phoneKey = `org:${orgId}:contact_by_phone:${contact.phone}`;
        allKvOperations.push({ type: 'put', key: phoneKey, value: contact, contactId: id });
      }
    });

    console.log(`📦 Executing ${allKvOperations.length} KV operations for ${contacts.length} contacts`);

    // OPTIMIZED: Execute all KV operations in larger parallel chunks (50 at a time)
    const KV_WRITE_CHUNK_SIZE = 50;
    const opResults = new Map(); // Track success/failure per contact

    for (let i = 0; i < allKvOperations.length; i += KV_WRITE_CHUNK_SIZE) {
      const chunk = allKvOperations.slice(i, i + KV_WRITE_CHUNK_SIZE);

      const chunkPromises = chunk.map(async (op) => {
        try {
          await this.kv.put(op.key, op.value);
          return { success: true, contactId: op.contactId };
        } catch (error) {
          console.error(`Failed KV operation for contact ${op.contactId}:`, error);
          return { success: false, contactId: op.contactId, error: error.message };
        }
      });

      const chunkResults = await Promise.allSettled(chunkPromises);

      // Track results per contact
      chunkResults.forEach(result => {
        if (result.status === 'fulfilled' && result.value) {
          const { contactId, success, error } = result.value;
          if (!opResults.has(contactId)) {
            opResults.set(contactId, { successes: 0, failures: 0, errors: [] });
          }
          if (success) {
            opResults.get(contactId).successes++;
          } else {
            opResults.get(contactId).failures++;
            opResults.get(contactId).errors.push(error);
          }
        }
      });
    }

    // Process final results per contact
    contacts.forEach(({ id }) => {
      const result = opResults.get(id);
      const contact = contactMap.get(id);

      if (result && result.failures === 0) {
        // All operations succeeded for this contact
        created.push(contact);
      } else {
        // At least one operation failed
        errors.push({
          id,
          error: result?.errors?.join(', ') || 'Failed to create contact'
        });
      }
    });

    return { created, errors };
  }

  async updateContact(orgId, contactId, updates, retries = 2) {
    try {
      const contactKey = `org:${orgId}:contact:${contactId}`;
      let existing = await this.kv.get(contactKey);

      // RACE CONDITION FIX: Retry if contact not found (might be mid-write from parallel batch)
      if (!existing && retries > 0) {
        console.log(`⚠️ Contact ${contactId} not found, retrying (${retries} attempts left)...`);
        await new Promise(resolve => setTimeout(resolve, 100)); // Wait 100ms
        return this.updateContact(orgId, contactId, updates, retries - 1);
      }

      if (!existing) {
        // After retries, if still not found, treat as warning not error
        console.warn(`⚠️ Contact ${contactId} not found after retries - may have been created by another batch`);
        return null;
      }

      const updated = {
        ...existing,
        ...updates,
        updatedAt: new Date().toISOString()
      };

      // Prepare parallel KV operations
      const kvOperations = [this.kv.put(contactKey, updated)];

      // Update indexes if email/phone changed
      if (updates.email && updates.email !== existing.email) {
        const emailKey = `org:${orgId}:contact_by_email:${updates.email.toLowerCase()}`;
        kvOperations.push(this.kv.put(emailKey, updated));
      }

      if (updates.phone && updates.phone !== existing.phone) {
        const phoneKey = `org:${orgId}:contact_by_phone:${updates.phone}`;
        kvOperations.push(this.kv.put(phoneKey, updated));
      }

      // Execute all updates in parallel
      await Promise.allSettled(kvOperations);

      return updated;
    } catch (error) {
      console.error(`Failed to update contact ${contactId}:`, error);
      throw error;
    }
  }

  async forceRebuildMetadata(orgId) {
    console.log(`🔄 WORKER - Invalidating contact cache for lazy rebuild by dashboard`);

    try {
      // STRATEGY: Instead of rebuilding indexes here (which causes KV rate limits),
      // we just INVALIDATE the cache. The dashboard will rebuild it lazily on first load.

      const metaKey = `org:${orgId}:contact_meta`;
      const searchKey = `org:${orgId}:contact_search`;

      // Get existing metadata to know how many pages to clear
      const existingMeta = await this.kv.getCache(metaKey);
      if (existingMeta) {
        const metadata = existingMeta;
        console.log(`🧹 WORKER - Clearing ${metadata.totalPages || 0} cached pages`);

        // Clear all cached page indexes
        for (let page = 1; page <= (metadata.totalPages || 0) + 1; page++) {
          const pageKey = `org:${orgId}:contact_index:page_${page}`;
          await this.kv.deleteCache(pageKey);
        }
      }

      // Delete metadata and search index
      await this.kv.deleteCache(metaKey);
      await this.kv.deleteCache(searchKey);

      console.log('✅ WORKER - Cache invalidated. Dashboard will rebuild on next load.');

      return { invalidated: true, message: 'Cache cleared for lazy rebuild' };
    } catch (error) {
      console.error('❌ WORKER - Cache invalidation failed:', error);
      throw error;
    }
  }
}

const METADATA_REBUILD_LIMIT_BUFFER = 75;
const METADATA_REBUILD_SCAN_LIMIT = 150;
const METADATA_CACHE_DELETE_BATCH = 25;
const METADATA_REBUILD_LOCK_TTL_MS = 30000;

const getMetadataRebuildProgressKey = (orgId, jobId) => `org:${orgId}:metadata_rebuild:${jobId}`;

const generateLockId = () => (typeof crypto !== 'undefined' && crypto.randomUUID
  ? crypto.randomUUID()
  : `${Date.now()}-${Math.random().toString(36).slice(2)}`);

const shouldYieldForKvBudget = (kvService, buffer = METADATA_REBUILD_LIMIT_BUFFER) =>
  (kvService.maxOperations - kvService.operationCount) <= buffer;

async function enqueueMetadataRebuild(kvService, params) {
  const { orgId, reason = 'general', uploadContext = null, finalizationKey = null } = params;
  let jobId = params.jobId;

  if (finalizationKey) {
    const finalizationState = await kvService.getCache(finalizationKey);
    if (finalizationState?.status === 'queued') {
      console.log('⚠️ METADATA REBUILD already queued:', { orgId, jobId: finalizationState.jobId, reason });
      return finalizationState.jobId;
    }
    if (finalizationState?.status === 'completed') {
      console.log('ℹ️ METADATA REBUILD already completed for finalization key', { orgId, reason });
      return finalizationState.jobId;
    }
  }

  jobId = jobId || `${reason}:${Date.now()}`;
  const progressKey = getMetadataRebuildProgressKey(orgId, jobId);
  const existingProgress = await kvService.getCache(progressKey);

  if (existingProgress && existingProgress.stage && existingProgress.stage !== 'completed') {
    console.log('⚠️ METADATA REBUILD already in progress:', { orgId, jobId: existingProgress.jobId, reason });
    return existingProgress.jobId;
  }

  if (existingProgress?.stage === 'completed') {
    console.log('ℹ️ METADATA REBUILD already completed for job', { orgId, jobId });
    return existingProgress.jobId;
  }

  const progressRecord = {
    orgId,
    jobId,
    stage: 'pending',
    reason,
    uploadContext,
    finalizationKey,
    startedAt: new Date().toISOString()
  };

  await kvService.setCache(progressKey, progressRecord, 7200);

  if (finalizationKey) {
    await kvService.setCache(finalizationKey, {
      status: 'queued',
      jobId,
      reason,
      updatedAt: new Date().toISOString()
    }, 7200);
  }

  await kvService.env.CONTACT_PROCESSING_QUEUE.send({
    type: MESSAGE_TYPES.METADATA_REBUILD,
    orgId,
    jobId,
    reason,
    uploadContext,
    finalizationKey
  });

  console.log('✅ METADATA REBUILD JOB QUEUED:', { orgId, jobId, reason });
  return jobId;
}

async function requeueMetadataRebuildJob(kvService, progressKey, progress, lockId) {
  if (progress.lockId === lockId) {
    delete progress.lockId;
    delete progress.lockedAt;
  }

  await kvService.setCache(progressKey, progress, 7200);

  await kvService.env.CONTACT_PROCESSING_QUEUE.send({
    type: MESSAGE_TYPES.METADATA_REBUILD,
    orgId: progress.orgId,
    jobId: progress.jobId,
    reason: progress.reason,
    uploadContext: progress.uploadContext,
    finalizationKey: progress.finalizationKey
  });

  console.log('🔁 METADATA REBUILD re-queued due to KV budget:', {
    orgId: progress.orgId,
    jobId: progress.jobId,
    stage: progress.stage,
    processedContacts: progress.processedContacts || 0
  });
}

async function markFinalizationComplete(kvService, finalizationKey, jobId) {
  if (!finalizationKey) return;

  await kvService.setCache(finalizationKey, {
    status: 'completed',
    jobId,
    completedAt: new Date().toISOString()
  }, 3600);
}

async function processMetadataRebuildJob(message, kvService) {
  const {
    orgId,
    jobId = `metadata:${Date.now()}`,
    reason = 'general',
    uploadContext = null,
    finalizationKey = null
  } = message;

  const metadataService = new ContactMetadataService(kvService);
  const progressKey = getMetadataRebuildProgressKey(orgId, jobId);
  let progress = await kvService.getCache(progressKey);

  if (progress && progress.stage === 'completed') {
    console.log('ℹ️ METADATA REBUILD already completed - skipping duplicate message', { orgId, jobId });
    await markFinalizationComplete(kvService, progress.finalizationKey || finalizationKey, jobId);
    return;
  }

  if (!progress) {
    progress = {
      orgId,
      jobId,
      stage: 'metadata',
      reason,
      metadata: metadataService.createEmptyMetadata(),
      cursor: undefined,
      processedContacts: 0,
      batchNumber: 0,
      uploadContext,
      finalizationKey,
      startedAt: new Date().toISOString()
    };
  } else {
    progress.stage = progress.stage && progress.stage !== 'pending' ? progress.stage : 'metadata';
    progress.metadata = progress.metadata || metadataService.createEmptyMetadata();
    progress.reason = progress.reason || reason;
    progress.uploadContext = progress.uploadContext || uploadContext || null;
    progress.finalizationKey = progress.finalizationKey || finalizationKey || null;
    progress.orgId = orgId;
    progress.jobId = jobId;
  }


  const targetListId = progress.uploadContext?.listId || uploadContext?.listId || null;
  if (targetListId) {
    progress.listAssignment = progress.listAssignment || { listId: targetListId, contactIds: [] };
    if (!progress.listAssignment.listId) {
      progress.listAssignment.listId = targetListId;
    }
  }

  const existingLockAge = progress.lockedAt ? (Date.now() - new Date(progress.lockedAt).getTime()) : Infinity;
  if (progress.lockId && existingLockAge < METADATA_REBUILD_LOCK_TTL_MS) {
    console.log('⏳ METADATA REBUILD already locked by another worker', { orgId, jobId });
    return;
  }

  const lockId = generateLockId();
  progress.lockId = lockId;
  progress.lockedAt = new Date().toISOString();
  await kvService.setCache(progressKey, progress, 7200);

  progress = await kvService.getCache(progressKey);
  if (!progress || progress.lockId !== lockId) {
    console.log('⚠️ METADATA REBUILD lock contention - deferring execution', { orgId, jobId });
    return;
  }

  progress.metadata = progress.metadata || metadataService.createEmptyMetadata();

  const listAssignment = progress.listAssignment || null;
  const listAssignmentSet = listAssignment ? new Set(listAssignment.contactIds || []) : null;

  const prefix = `org:${orgId}:contact:`;

  if (progress.stage === 'metadata') {
    while (true) {
      const listOptions = { prefix, limit: METADATA_REBUILD_SCAN_LIMIT };
      if (progress.cursor) {
        listOptions.cursor = progress.cursor;
      }

      const list = await kvService.listMain(listOptions);
      progress.batchNumber = (progress.batchNumber || 0) + 1;
      console.log(`📊 METADATA REBUILD JOB ${jobId} - Batch ${progress.batchNumber}: ${list.keys.length} contacts`);

      for (const key of list.keys) {
        const contact = await kvService.get(key.name);
        if (contact) {
          metadataService.applyAddOperation(progress.metadata, contact);

          if (listAssignment && Array.isArray(contact.contactListIds) && contact.contactListIds.includes(listAssignment.listId)) {
            if (!listAssignmentSet.has(contact.id)) {
              listAssignmentSet.add(contact.id);
              listAssignment.contactIds.push(contact.id);
            }
          }
        }
      }

      progress.processedContacts = (progress.processedContacts || 0) + list.keys.length;
      progress.cursor = list.list_complete ? undefined : list.cursor;
      await kvService.setCache(progressKey, progress, 7200);

      if (list.list_complete) {
        break;
      }

      if (shouldYieldForKvBudget(kvService)) {
        await requeueMetadataRebuildJob(kvService, progressKey, progress, lockId);
        return;
      }
    }

    progress.metadata.lastUpdated = new Date().toISOString();
    progress.metadata.version = (progress.metadata.version || 0) + 1;
    await kvService.put(`org:${orgId}:contact_metadata`, progress.metadata);

    progress.stage = 'cache_invalidation';
    progress.cursor = undefined;
    progress.cacheInvalidation = progress.cacheInvalidation || {
      totalPages: 0,
      nextPage: 1,
      clearedMeta: false,
      clearedSearch: false
    };

    const cachedMeta = await kvService.getCache(`org:${orgId}:contact_meta`);
    progress.cacheInvalidation.totalPages = cachedMeta?.totalPages || 0;
    await kvService.setCache(progressKey, progress, 7200);
  }

  if (progress.stage === 'cache_invalidation') {
    const cacheState = progress.cacheInvalidation || {
      totalPages: 0,
      nextPage: 1,
      clearedMeta: false,
      clearedSearch: false
    };

    while (cacheState.nextPage <= (cacheState.totalPages || 0) + 1) {
      const pageKey = `org:${orgId}:contact_index:page_${cacheState.nextPage}`;
      await kvService.deleteCache(pageKey);
      cacheState.nextPage++;

      if ((cacheState.nextPage - 1) % METADATA_CACHE_DELETE_BATCH === 0 && shouldYieldForKvBudget(kvService)) {
        progress.cacheInvalidation = cacheState;
        await requeueMetadataRebuildJob(kvService, progressKey, progress, lockId);
        return;
      }
    }

    const metaKey = `org:${orgId}:contact_meta`;
    const searchKey = `org:${orgId}:contact_search`;

    if (!cacheState.clearedMeta) {
      await kvService.deleteCache(metaKey);
      cacheState.clearedMeta = true;
      if (shouldYieldForKvBudget(kvService)) {
        progress.cacheInvalidation = cacheState;
        await requeueMetadataRebuildJob(kvService, progressKey, progress, lockId);
        return;
      }
    }

    if (!cacheState.clearedSearch) {
      await kvService.deleteCache(searchKey);
      cacheState.clearedSearch = true;
      if (shouldYieldForKvBudget(kvService)) {
        progress.cacheInvalidation = cacheState;
        await requeueMetadataRebuildJob(kvService, progressKey, progress, lockId);
        return;
      }
    }

    progress.cacheInvalidation = cacheState;
    progress.stage = 'completed';
    progress.completedAt = new Date().toISOString();
  }

  if (progress.stage === 'completed') {
    if (progress.lockId === lockId) {
      delete progress.lockId;
      delete progress.lockedAt;
    }

    await kvService.setCache(progressKey, progress, 7200);

    if (progress.uploadContext?.uploadId) {
      const { uploadId, listId, totalResults } = progress.uploadContext;
      const uploadKey = `org:${orgId}:upload_status:${uploadId}`;
      await kvService.setCache(uploadKey, {
        status: 'complete',
        stage: 'complete',
        processed: totalResults.contacts,
        total: totalResults.contacts,
        results: {
          listId,
          totalRows: totalResults.contacts,
          successfulRows: totalResults.created,
          duplicatesUpdated: totalResults.duplicatesUpdated,
          skippedDuplicates: totalResults.skippedDuplicates,
          failedRows: totalResults.errors,
          errors: []
        },
        completedAt: new Date().toISOString()
      }, 7200);
    }

    if (progress.listAssignment?.listId) {
      await assignContactsToList(kvService, orgId, progress.listAssignment.listId, progress.listAssignment.contactIds || []);
    }

    await markFinalizationComplete(kvService, progress.finalizationKey, jobId);
    console.log('✅ METADATA REBUILD JOB COMPLETE:', {
      orgId,
      jobId,
      processedContacts: progress.processedContacts || 0,
      reason: progress.reason
    });
  }
}

// Campaign sending processor
async function processCampaignBatch(message, kvService) {
  const { 
    campaignId, orgId, batchNumber, totalBatches, contacts, 
    campaign, salesMembers = [], messagingEndpoints = {} 
  } = message;
  
  console.log(`📧 CAMPAIGN BATCH ${batchNumber}/${totalBatches} - Processing ${contacts.length} contacts for campaign ${campaignId}`);
  
  try {
    const results = {
      sent: 0,
      failed: 0,
      errors: []
    };
    
    // Process contacts in smaller batches to avoid overwhelming external APIs (tunable)
    const SEND_BATCH_SIZE = CAMPAIGN_SEND_BATCH_SIZE;
    
    for (let i = 0; i < contacts.length; i += SEND_BATCH_SIZE) {
      const batch = contacts.slice(i, i + SEND_BATCH_SIZE);
      
      const batchPromises = batch.map(async (contact, index) => {
        try {
          // Skip opted-out contacts
          if (contact.optedOut) {
            console.log(`📧 Skipping opted-out contact: ${contact.id}`);
            return { success: true, skipped: true };
          }
          
          // Get sales member for this contact if applicable
          let salesMember = null;
          if (campaign.campaignType === 'sales' && salesMembers.length > 0) {
            salesMember = salesMembers[(i + index) % salesMembers.length]; // Round robin
          }
          
          const sendPromises = [];
          
          // Send email if campaign includes email
          if ((campaign.type === 'email' || campaign.type === 'both') && 
              campaign.emailTemplate && contact.email) {
            sendPromises.push(
              sendEmailWithRateLimit(kvService, orgId, contact, campaign, salesMember, messagingEndpoints.email)
            );
          }
          
          // Send SMS if campaign includes SMS
          if ((campaign.type === 'sms' || campaign.type === 'both') && 
              campaign.smsTemplate && contact.phone) {
            sendPromises.push(
              sendSMS(contact, campaign, salesMember, messagingEndpoints.sms)
            );
          }
          
          await Promise.all(sendPromises);
          
          // Track delivery in KV
          await trackCampaignDelivery(kvService, orgId, campaignId, contact.id, {
            sentAt: new Date().toISOString(),
            status: 'sent',
            channels: campaign.type
          });
          
          return { success: true };
          
        } catch (error) {
          console.error(`📧 Failed to send campaign to contact ${contact.id}:`, error);
          return { 
            success: false, 
            contactId: contact.id, 
            error: error.message 
          };
        }
      });
      
      const batchResults = await Promise.all(batchPromises);
      
      // Process results
      batchResults.forEach(result => {
        if (result.success && !result.skipped) {
          results.sent++;
        } else if (!result.success) {
          results.failed++;
          results.errors.push({
            contactId: result.contactId,
            error: result.error
          });
        }
      });
      
      // Delay between send batches to respect API limits
      if (i + SEND_BATCH_SIZE < contacts.length) {
        await new Promise(resolve => setTimeout(resolve, CAMPAIGN_BATCH_DELAY_MS));
      }
    }
    
    // Update batch status
    await updateBatchStatus(kvService, orgId, `campaign_${campaignId}`, batchNumber, 'completed', {
      sent: results.sent,
      failed: results.failed,
      errors: results.errors.length,
      completedAt: new Date().toISOString()
    });
    
    console.log(`✅ CAMPAIGN BATCH ${batchNumber}/${totalBatches} - Completed:`, results);
    
    // Check if this was the last batch and finalize campaign
    await checkAndFinalizeCampaign(kvService, orgId, campaignId, totalBatches);
    
  } catch (error) {
    console.error(`❌ CAMPAIGN BATCH ${batchNumber}/${totalBatches} - Failed:`, error);
    
    await updateBatchStatus(kvService, orgId, `campaign_${campaignId}`, batchNumber, 'failed', {
      error: error.message,
      failedAt: new Date().toISOString()
    });
    
    throw error;
  }
}

async function sendEmailWithRateLimit(kvService, orgId, contact, campaign, salesMember, emailEndpoint) {
  await waitForMailgunCapacity(kvService, orgId, 1);
  await sendEmail(contact, campaign, salesMember, emailEndpoint);
}

async function waitForMailgunCapacity(kvService, orgId, requestedCount = 1) {
  if (!requestedCount || requestedCount <= 0) {
    return;
  }

  const key = getMailgunRateLimitKey(orgId);

  while (true) {
    const now = Date.now();
    let record = await kvService.getCache(key);

    if (!record || !record.windowStart || (now - record.windowStart) >= MAILGUN_RATE_LIMIT_WINDOW_MS) {
      record = { windowStart: now, count: 0 };
    }

    if ((record.count || 0) + requestedCount <= MAILGUN_MAX_EMAILS_PER_MINUTE) {
      record.count = (record.count || 0) + requestedCount;
      record.windowStart = record.windowStart || now;
      await kvService.setCache(key, record, Math.ceil(MAILGUN_RATE_LIMIT_WINDOW_MS / 1000) * 2);
      return;
    }

    const waitMs = Math.max(
      200,
      MAILGUN_RATE_LIMIT_WINDOW_MS - (now - record.windowStart) + 50
    );

    console.log('MAILGUN RATE LIMIT - pausing campaign send', {
      orgId,
      requestedCount,
      currentCount: record.count,
      waitMs
    });

    await new Promise(resolve => setTimeout(resolve, Math.min(waitMs, 2000)));
  }
}

function getMailgunRateLimitKey(orgId) {
  return `org:${orgId}:mailgun_rate_limit`;
}

// Helper functions for campaign sending
async function sendEmail(contact, campaign, salesMember, emailEndpoint) {
  if (!emailEndpoint) {
    throw new Error('Email endpoint not configured');
  }
  
  // Replace template variables
  let emailContent = campaign.emailTemplate
    .replace(/\{firstName\}/g, contact.firstName || '')
    .replace(/\{lastName\}/g, contact.lastName || '')
    .replace(/\{email\}/g, contact.email || '');
  
  // Replace custom metadata variables
  if (contact.metadata) {
    Object.entries(contact.metadata).forEach(([key, value]) => {
      emailContent = emailContent.replace(new RegExp(`\\{${key}\\}`, 'g'), value || '');
    });
  }
  
  // Replace sales member variables if applicable
  if (salesMember) {
    emailContent = emailContent
      .replace(/\{salesPersonName\}/g, salesMember.firstName + ' ' + salesMember.lastName)
      .replace(/\{salesPersonEmail\}/g, salesMember.email || '')
      .replace(/\{salesPersonPhone\}/g, salesMember.phone || '');
  }
  
  const response = await fetch(emailEndpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      to: contact.email,
      subject: campaign.emailSubject || 'Message from ' + (salesMember?.firstName || 'Our Team'),
      content: emailContent,
      fromName: salesMember?.firstName + ' ' + salesMember?.lastName || 'Our Team'
    })
  });
  
  if (!response.ok) {
    throw new Error(`Email API error: ${response.status}`);
  }
}

async function sendSMS(contact, campaign, salesMember, smsEndpoint) {
  if (!smsEndpoint) {
    throw new Error('SMS endpoint not configured');
  }
  
  // Replace template variables
  let smsContent = campaign.smsTemplate
    .replace(/\{firstName\}/g, contact.firstName || '')
    .replace(/\{lastName\}/g, contact.lastName || '');
  
  // Replace custom metadata variables
  if (contact.metadata) {
    Object.entries(contact.metadata).forEach(([key, value]) => {
      smsContent = smsContent.replace(new RegExp(`\\{${key}\\}`, 'g'), value || '');
    });
  }
  
  // Replace sales member variables if applicable
  if (salesMember) {
    smsContent = smsContent
      .replace(/\{salesPersonName\}/g, salesMember.firstName + ' ' + salesMember.lastName)
      .replace(/\{salesPersonPhone\}/g, salesMember.phone || '');
  }
  
  const response = await fetch(smsEndpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      to: contact.phone,
      message: smsContent,
      from: salesMember?.phone || '+1234567890' // Default number
    })
  });
  
  if (!response.ok) {
    throw new Error(`SMS API error: ${response.status}`);
  }
}

async function trackCampaignDelivery(kvService, orgId, campaignId, contactId, deliveryData) {
  const deliveryKey = `org:${orgId}:delivery:${campaignId}:${contactId}`;
  await kvService.put(deliveryKey, deliveryData);
}

async function checkAndFinalizeCampaign(kvService, orgId, campaignId, totalBatches) {
  try {
    // Check if all batches are completed
    const completedBatches = [];
    for (let i = 1; i <= totalBatches; i++) {
      const batchKey = `org:${orgId}:campaign_batch:campaign_${campaignId}:${i}`;
      const batchStatus = await kvService.getCache(batchKey);
      if (batchStatus?.status === 'completed') {
        completedBatches.push(batchStatus);
      }
    }
    
    if (completedBatches.length === totalBatches) {
      console.log('🎉 ALL CAMPAIGN BATCHES COMPLETED - Finalizing campaign');
      
      // Aggregate results
      const totalResults = completedBatches.reduce((acc, batch) => ({
        sent: acc.sent + (batch.sent || 0),
        failed: acc.failed + (batch.failed || 0),
        errors: acc.errors + (batch.errors || 0)
      }), { sent: 0, failed: 0, errors: 0 });
      
      // Update campaign status
      const campaignKey = `org:${orgId}:campaign:${campaignId}`;
      const campaign = await kvService.get(campaignKey);
      if (campaign) {
        campaign.status = 'completed';
        campaign.completedAt = new Date().toISOString();
        campaign.results = totalResults;
        await kvService.put(campaignKey, campaign);
      }
      
      console.log('✅ CAMPAIGN FINALIZATION COMPLETE:', totalResults);
    }
  } catch (error) {
    console.error('❌ CAMPAIGN FINALIZATION FAILED:', error);
  }
}

// Opt-out batch processor
async function processOptOutBatch(message, kvService) {
  const { orgId, batchNumber, totalBatches, optOuts } = message;
  
  console.log(`📱 OPT-OUT BATCH ${batchNumber}/${totalBatches} - Processing ${optOuts.length} opt-outs`);
  
  try {
    let processedCount = 0;
    let errorCount = 0;
    
    const contactService = new ContactService(kvService.env);
    
    // Process opt-outs in small batches to avoid rate limits
    const OPT_OUT_BATCH_SIZE = 10;
    
    for (let i = 0; i < optOuts.length; i += OPT_OUT_BATCH_SIZE) {
      const batch = optOuts.slice(i, i + OPT_OUT_BATCH_SIZE);
      
      const batchPromises = batch.map(async (optOut) => {
        try {
          const { email, phone, contactId } = optOut;
          let contact = null;
          
          // Find contact by ID, email, or phone
          if (contactId) {
            const contactData = await kvService.get(`org:${orgId}:contact:${contactId}`);
            contact = contactData ? { ...contactData, id: contactId } : null;
          } else if (email || phone) {
            // Search for contact by email/phone
            contact = await contactService.findContactByEmailOrPhone(orgId, email, phone);
          }
          
          if (contact) {
            // Update contact opt-out status
            const updatedContact = {
              ...contact,
              optedOut: true,
              optedOutAt: new Date().toISOString(),
              updatedAt: new Date().toISOString()
            };
            
            const contactKey = `org:${orgId}:contact:${contact.id}`;
            await kvService.put(contactKey, updatedContact);

            // NOTE: Metadata incremental updates removed to avoid race conditions
            // Metadata will be rebuilt from scratch after all batches complete

            console.log(`✅ Opted out contact: ${contact.id} (${email || phone})`);
            return { success: true };
          } else {
            console.warn(`⚠️ Contact not found for opt-out: ${email || phone}`);
            return { success: false, error: 'Contact not found' };
          }
          
        } catch (error) {
          console.error(`❌ Failed to process opt-out:`, error);
          return { success: false, error: error.message };
        }
      });
      
      const results = await Promise.all(batchPromises);
      results.forEach(result => {
        if (result.success) {
          processedCount++;
        } else {
          errorCount++;
        }
      });
      
      // Small delay between batches
      if (i + OPT_OUT_BATCH_SIZE < optOuts.length) {
        await new Promise(resolve => setTimeout(resolve, 200));
      }
    }
    
    console.log(`✅ OPT-OUT BATCH ${batchNumber}/${totalBatches} - Processed: ${processedCount}, Errors: ${errorCount}`);

    // Mark batch as completed
    await updateBatchStatus(kvService, orgId, 'opt_out', batchNumber, 'completed', {
      processed: processedCount,
      errors: errorCount,
      completedAt: new Date().toISOString()
    });

    // Check if all batches are complete and finalize
    await checkAndFinalizeOptOutBatches(kvService, orgId, totalBatches);

  } catch (error) {
    console.error(`❌ OPT-OUT BATCH ${batchNumber}/${totalBatches} - Failed:`, error);
    throw error;
  }
}

// Check and finalize opt-out batches
async function checkAndFinalizeOptOutBatches(kvService, orgId, totalBatches) {
  try {
    // Check if all batches are completed
    const completedBatches = [];
    for (let i = 1; i <= totalBatches; i++) {
      const batchKey = `org:${orgId}:opt_out_batch:opt_out:${i}`;
      const batchStatus = await kvService.getCache(batchKey);
      if (batchStatus?.status === 'completed') {
        completedBatches.push(batchStatus);
      }
    }

    console.log(`📊 OPT-OUT FINALIZE CHECK - Batches completed: ${completedBatches.length}/${totalBatches}`);

    if (completedBatches.length === totalBatches) {
      console.log('🎉 ALL OPT-OUT BATCHES COMPLETED - Starting finalization');

      const finalizationKey = `org:${orgId}:opt_out_finalization`;
      const jobId = await enqueueMetadataRebuild(kvService, {
        orgId,
        reason: 'opt_out',
        finalizationKey
      });

      console.log('✅ OPT-OUT FINALIZATION - Metadata rebuild queued', { orgId, jobId });
    }
  } catch (error) {
    console.error('❌ OPT-OUT FINALIZATION FAILED:', error);
  }
}

// Contact deletion processor
async function processContactDeletion(message, kvService) {
  const { orgId, batchNumber, totalBatches, contactKeys } = message;
  
  console.log(`🗑️ DELETE BATCH ${batchNumber}/${totalBatches} - Deleting ${contactKeys.length} contact keys`);
  
  try {
    let deletedCount = 0;
    let errorCount = 0;
    
    // Delete in small batches to avoid rate limits
    const DELETE_BATCH_SIZE = 20;
    
    for (let i = 0; i < contactKeys.length; i += DELETE_BATCH_SIZE) {
      const batch = contactKeys.slice(i, i + DELETE_BATCH_SIZE);
      
      const deletePromises = batch.map(async (key) => {
        try {
          await kvService.main.delete(key);
          return { success: true };
        } catch (error) {
          console.error(`Failed to delete key ${key}:`, error);
          return { success: false, error };
        }
      });
      
      const results = await Promise.all(deletePromises);
      results.forEach(result => {
        if (result.success) {
          deletedCount++;
        } else {
          errorCount++;
        }
      });
      
      // Small delay between batches
      if (i + DELETE_BATCH_SIZE < contactKeys.length) {
        await new Promise(resolve => setTimeout(resolve, 200));
      }
    }
    
    console.log(`✅ DELETE BATCH ${batchNumber}/${totalBatches} - Deleted: ${deletedCount}, Errors: ${errorCount}`);

    // Mark batch as completed
    await updateBatchStatus(kvService, orgId, 'deletion', batchNumber, 'completed', {
      deleted: deletedCount,
      errors: errorCount,
      completedAt: new Date().toISOString()
    });

    // Check if all batches are complete and finalize
    await checkAndFinalizeContactDeletion(kvService, orgId, totalBatches);

  } catch (error) {
    console.error(`❌ DELETE BATCH ${batchNumber}/${totalBatches} - Failed:`, error);
    throw error;
  }
}

// Check and finalize contact deletion batches
async function checkAndFinalizeContactDeletion(kvService, orgId, totalBatches) {
  try {
    // Check if all batches are completed
    const completedBatches = [];
    for (let i = 1; i <= totalBatches; i++) {
      const batchKey = `org:${orgId}:deletion_batch:deletion:${i}`;
      const batchStatus = await kvService.getCache(batchKey);
      if (batchStatus?.status === 'completed') {
        completedBatches.push(batchStatus);
      }
    }

    console.log(`📊 DELETION FINALIZE CHECK - Batches completed: ${completedBatches.length}/${totalBatches}`);

    if (completedBatches.length === totalBatches) {
      console.log('🎉 ALL DELETION BATCHES COMPLETED - Starting finalization');

      const finalizationKey = `org:${orgId}:deletion_finalization`;
      const jobId = await enqueueMetadataRebuild(kvService, {
        orgId,
        reason: 'contact_deletion',
        finalizationKey
      });

      console.log('✅ DELETION FINALIZATION - Metadata rebuild queued', { orgId, jobId });
    }
  } catch (error) {
    console.error('❌ DELETION FINALIZATION FAILED:', error);
  }
}

// Bulk delete all org data (worker enumerates and deletes in batches)
async function processBulkDeleteOrgData(message, kvService) {
  const { orgId, deletionId, prefixesToDelete } = message;

  console.log(`🗑️ BULK DELETE ORG DATA - Starting deletion for org: ${orgId}, deletionId: ${deletionId}`);
  console.log(`🗑️ BULK DELETE - Prefixes to delete:`, prefixesToDelete);

  try {
    const progressKey = `org:${orgId}:deletion_progress:${deletionId}`;

    // Get or initialize deletion progress
    let progress = await kvService.getCache(progressKey);
    if (!progress) {
      progress = {
        deletionId,
        orgId,
        prefixesToDelete,
        processedPrefixes: [],
        totalDeleted: 0,
        prefixState: {},
        startedAt: new Date().toISOString()
      };
    }

    progress.prefixState = progress.prefixState || {};

    console.log(`🗑️ BULK DELETE - Progress: ${progress.processedPrefixes.length}/${prefixesToDelete.length} prefixes complete, ${progress.totalDeleted} keys deleted`);

    const BULK_DELETE_LIST_LIMIT = 200;
    const BULK_DELETE_DELETE_BATCH = 50;
    const BULK_DELETE_MAX_DELETES = 800;
    const BULK_DELETE_KV_BUFFER = 100;

    let deletedThisInvocation = 0;
    let needsContinuation = false;

    // Process each prefix that hasn't been completed yet
    for (const prefix of prefixesToDelete) {
      // Skip if already processed
      if (progress.processedPrefixes.includes(prefix)) {
        console.log(`⏭️ BULK DELETE - Skipping already processed prefix: ${prefix}`);
        continue;
      }

      console.log(`🗑️ BULK DELETE - Processing prefix: ${prefix}`);

      const prefixState = progress.prefixState[prefix] || { cursor: undefined };
      let continuePrefix = true;

      while (continuePrefix) {
        if (deletedThisInvocation >= BULK_DELETE_MAX_DELETES || shouldYieldForKvBudget(kvService, BULK_DELETE_KV_BUFFER)) {
          needsContinuation = true;
          break;
        }

        const listOptions = { prefix, limit: BULK_DELETE_LIST_LIMIT };
        if (prefixState.cursor) {
          listOptions.cursor = prefixState.cursor;
        }

        const list = await kvService.listMain(listOptions);
        console.log(`📋 BULK DELETE - Found ${list.keys.length} keys for prefix: ${prefix} (list_complete: ${list.list_complete}, cursor: ${!!list.cursor})`);

        if (list.keys.length === 0) {
          if (list.list_complete) {
            progress.processedPrefixes.push(prefix);
            delete progress.prefixState[prefix];
            console.log(`✅ BULK DELETE - Prefix complete (no keys found): ${prefix}`);
          } else {
            prefixState.cursor = list.cursor;
            progress.prefixState[prefix] = prefixState;
            needsContinuation = true;
          }
          break;
        }

        const keyNames = list.keys.map(key => key.name);
        for (let i = 0; i < keyNames.length; i += BULK_DELETE_DELETE_BATCH) {
          const chunk = keyNames.slice(i, i + BULK_DELETE_DELETE_BATCH);
          await Promise.allSettled(chunk.map(keyName => kvService.delete(keyName)));
          deletedThisInvocation += chunk.length;
          progress.totalDeleted += chunk.length;

          if (deletedThisInvocation >= BULK_DELETE_MAX_DELETES || shouldYieldForKvBudget(kvService, BULK_DELETE_KV_BUFFER)) {
            prefixState.cursor = list.list_complete && (i + BULK_DELETE_DELETE_BATCH >= keyNames.length) ? undefined : list.cursor;
            progress.prefixState[prefix] = prefixState;
            needsContinuation = true;
            continuePrefix = false;
            break;
          }
        }

        if (!continuePrefix && needsContinuation) {
          break;
        }

        if (list.list_complete) {
          progress.processedPrefixes.push(prefix);
          delete progress.prefixState[prefix];
          console.log(`✅ BULK DELETE - Prefix complete: ${prefix}`);
          break;
        } else {
          prefixState.cursor = list.cursor;
          progress.prefixState[prefix] = prefixState;
          // Continue loop to process next page if we still have budget
          if (deletedThisInvocation >= BULK_DELETE_MAX_DELETES || shouldYieldForKvBudget(kvService, BULK_DELETE_KV_BUFFER)) {
            needsContinuation = true;
            break;
          }
        }
      }

      if (needsContinuation) {
        break;
      }
    }

    // Save progress
    await kvService.setCache(progressKey, progress, 3600);

    // Check if all prefixes are processed
    if (progress.processedPrefixes.length === prefixesToDelete.length) {
      const finalizationKey = `org:${orgId}:bulk_delete_finalization:${deletionId}`;
      const jobId = await enqueueMetadataRebuild(kvService, {
        orgId,
        jobId: `bulk_delete:${deletionId}`,
        reason: 'bulk_delete',
        finalizationKey
      });

      await kvService.deleteCache(progressKey);

      console.log(`✅ BULK DELETE ORG DATA COMPLETE - Deleted ${progress.totalDeleted} keys for org: ${orgId}. Metadata job queued: ${jobId}`);
    } else if (needsContinuation) {
      // Queue a new message to continue deletion (avoid retry limits)
      console.log(`🔄 BULK DELETE - Queueing continuation message (${progress.processedPrefixes.length}/${prefixesToDelete.length} prefixes complete)`);

      await kvService.env.CONTACT_PROCESSING_QUEUE.send({
        type: 'bulk_delete_org_data',
        orgId,
        deletionId,
        prefixesToDelete
      });

      console.log(`✅ BULK DELETE - Continuation message queued successfully`);
    }

  } catch (error) {
    console.error(`❌ BULK DELETE ORG DATA - Failed for org ${orgId}:`, error);
    throw error;
  }
}

async function processContactBatch(message, kvService) {
  const { uploadId, orgId, listId, listName, batchNumber, totalBatches, contacts, reactivateDuplicates } = message;
  
  console.log(`📦 WORKER BATCH ${batchNumber}/${totalBatches} - Processing ${contacts.length} contacts for upload ${uploadId}`);
  console.log(`📊 Starting KV Operations: ${kvService.operationCount}/${kvService.maxOperations}`);
  
  try {
    const contactService = new ContactService(kvService.env);
    
    // Calculate approximate KV operations needed
    const estimatedOpsPerContact = 5; // 2-3 reads + 3 writes
    const estimatedTotalOps = contacts.length * estimatedOpsPerContact;
    
    if (kvService.operationCount + estimatedTotalOps > kvService.maxOperations) {
      // Process only what we can within the limit
      const remainingOps = kvService.maxOperations - kvService.operationCount;
      const maxContacts = Math.floor(remainingOps / estimatedOpsPerContact);
      
      if (maxContacts <= 0) {
        console.warn(`⚠️ WORKER BATCH ${batchNumber}/${totalBatches} - KV limit reached, deferring batch`);
        throw new Error('KV rate limit reached - batch will be retried');
      }
      
      console.warn(`⚠️ WORKER BATCH ${batchNumber}/${totalBatches} - Processing only ${maxContacts}/${contacts.length} contacts to avoid rate limit`);
      // Process partial batch - remaining contacts will be retried in next invocation
      contacts.splice(maxContacts);
    }
    
    // Update status to show this batch is processing
    await updateBatchStatus(kvService, orgId, uploadId, batchNumber, 'processing', {
      contacts: contacts.length,
      startTime: new Date().toISOString()
    });

    // PHASE 3: Use isDuplicate flags from message (duplicate checking done upfront in upload handler)
    console.log(`📊 PHASE 3 - Using pre-computed duplicate flags from upload handler`);

    // Fetch all existing contacts for duplicates in this batch (in parallel)
    const duplicateContactIds = contacts
      .filter(c => c.isDuplicate && c.existingContactId)
      .map(c => c.existingContactId);

    const existingContactMap = new Map();
    if (duplicateContactIds.length > 0) {
      console.log(`📦 PHASE 3 - Fetching ${duplicateContactIds.length} existing contacts for duplicates`);

      // Fetch all duplicate contacts in parallel
      const existingContactPromises = duplicateContactIds.map(async (contactId) => {
        try {
          const contactKey = `org:${orgId}:contact:${contactId}`;
          const contact = await contactService.kv.get(contactKey);
          if (contact) {
            return { id: contactId, contact };
          }
        } catch (error) {
          console.error(`Failed to fetch contact ${contactId}:`, error);
        }
        return null;
      });

      const existingContactsResults = await Promise.all(existingContactPromises);

      existingContactsResults.forEach(result => {
        if (result && result.contact) {
          existingContactMap.set(result.id, result.contact);
        }
      });

      console.log(`✅ PHASE 3 - Fetched ${existingContactMap.size} existing contacts`);
    }

    // Separate new contacts from duplicates based on pre-computed flags
    const newContacts = [];
    const duplicateUpdates = [];
    const assignedContactIds = [];

    for (const {rowIndex, contact, contactId, isDuplicate, existingContactId, finalContactId} of contacts) {
      const resolvedFinalContactId = finalContactId || existingContactId || contactId;

      if (isDuplicate && existingContactId) {
        const existingContact = existingContactMap.get(existingContactId);

        if (existingContact) {
          // Handle duplicate - merge data
          const updatedListIds = existingContact.contactListIds || [];
          if (!updatedListIds.includes(listId)) {
            updatedListIds.push(listId);
          }

          const mergedMetadata = { ...existingContact.metadata, ...contact.metadata };

          const updates = {
            firstName: contact.firstName || existingContact.firstName,
            lastName: contact.lastName || existingContact.lastName,
            email: contact.email || existingContact.email,
            phone: contact.phone || existingContact.phone,
            metadata: mergedMetadata,
            contactListIds: updatedListIds
          };

          if (reactivateDuplicates && existingContact.optedOut) {
            updates.optedOut = false;
            updates.optedOutAt = null;
          }

          duplicateUpdates.push({contact: existingContact, updates, finalContactId: resolvedFinalContactId || existingContact.id});
        }
      } else {
        // New contact
        newContacts.push({
          id: contactId,
          data: contact
        });
      }
    }
    
    console.log(`📊 WORKER BATCH ${batchNumber}/${totalBatches} - Split contacts:`, {
      newContacts: newContacts.length,
      duplicateUpdates: duplicateUpdates.length
    });
    
    // Process new contacts
    const results = { created: [], errors: [] };
    if (newContacts.length > 0) {
      const bulkResults = await contactService.createContactsBulk(orgId, newContacts);
      results.created.push(...bulkResults.created);
      results.errors.push(...bulkResults.errors);
      assignedContactIds.push(...bulkResults.created);
    }
    
    // Process duplicate updates in parallel batches
    let duplicatesUpdated = 0;
    let skippedDuplicates = 0;
    
    const DUPLICATE_BATCH_SIZE = 5; // Smaller batches for updates
    
    for (let i = 0; i < duplicateUpdates.length; i += DUPLICATE_BATCH_SIZE) {
      const batch = duplicateUpdates.slice(i, i + DUPLICATE_BATCH_SIZE);
      
      const updatePromises = batch.map(async ({contact, updates, finalContactId}) => {
        try {
          const result = await contactService.updateContact(orgId, contact.id, updates);
          // If result is null, contact wasn't found after retries (race condition)
          if (result === null) {
            console.warn(`⚠️ Skipping update for contact ${contact.id} - not found after retries`);
            return { success: true, skipped: true };
          }
          return {
            success: true,
            reactivated: reactivateDuplicates && contact.optedOut,
            finalContactId: finalContactId || contact.id
          };
        } catch (error) {
          console.error(`Failed to update duplicate contact ${contact.id}:`, error);
          return {
            success: false,
            error: { id: contact.id, error: "Failed to update duplicate" }
          };
        }
      });
      
      const batchResults = await Promise.allSettled(updatePromises);
      
      batchResults.forEach(result => {
        if (result.status === 'fulfilled') {
          if (result.value.success) {
            if (!result.value.skipped && result.value.finalContactId) {
              assignedContactIds.push(result.value.finalContactId);
            }
            if (result.value.skipped) {
              // Contact wasn't found (race condition) - don't count as error
              skippedDuplicates++;
            } else if (result.value.reactivated) {
              duplicatesUpdated++;
            } else {
              skippedDuplicates++;
            }
          } else {
            results.errors.push(result.value.error);
          }
        } else {
          results.errors.push({ id: 'unknown', error: result.reason?.message || 'Update failed' });
        }
      });
    }
    
    const uniqueAssignedContactIds = Array.from(new Set(assignedContactIds));
    // ALWAYS write completion status so finalization can track progress accurately
    await updateBatchStatus(kvService, orgId, uploadId, batchNumber, 'completed', {
      contacts: contacts.length,
      contactIds: uniqueAssignedContactIds,
      created: results.created.length,
      duplicatesUpdated,
      skippedDuplicates,
      errors: results.errors.length,
      completedAt: new Date().toISOString()
    });
    
    console.log(`✅ WORKER BATCH ${batchNumber}/${totalBatches} - Completed:`, {
      created: results.created.length,
      duplicatesUpdated,
      skippedDuplicates,
      errors: results.errors.length,
      kvOperationsUsed: kvService.operationCount,
      kvOperationsRemaining: kvService.maxOperations - kvService.operationCount
    });

    // NOTE: Metadata incremental updates removed to avoid race conditions
    // Metadata will be rebuilt from scratch after all batches complete in finalization

    // Check if this was the last batch and trigger final processing
    await checkAndFinalizeBatch(kvService, orgId, uploadId, listId, totalBatches);
    
  } catch (error) {
    console.error(`❌ WORKER BATCH ${batchNumber}/${totalBatches} - Failed:`, error);
    console.error(`❌ WORKER BATCH ${batchNumber}/${totalBatches} - Error stack:`, error instanceof Error ? error.stack : 'No stack trace');
    console.error(`❌ WORKER BATCH ${batchNumber}/${totalBatches} - Error details:`, {
      message: error instanceof Error ? error.message : String(error),
      name: error instanceof Error ? error.name : 'Unknown',
      uploadId,
      orgId,
      contactsInBatch: contacts.length
    });

    await updateBatchStatus(kvService, orgId, uploadId, batchNumber, 'failed', {
      error: error instanceof Error ? error.message : 'Unknown error',
      errorStack: error instanceof Error ? error.stack : undefined,
      failedAt: new Date().toISOString()
    });

    throw error; // Let queue retry
  }
}

async function updateBatchStatus(kvService, orgId, uploadId, batchNumber, status, data) {
  const batchKey = `org:${orgId}:upload_batch:${uploadId}:${batchNumber}`;
  const batchStatus = {
    status,
    batchNumber,
    updatedAt: new Date().toISOString(),
    ...data
  };
  
  await kvService.setCache(batchKey, batchStatus, 7200);
}

function normalizeContactIdsForList(contactIds = []) {
  const uniqueIds = new Set();
  for (const id of contactIds || []) {
    if (typeof id !== 'string') {
      continue;
    }
    const trimmed = id.trim();
    if (trimmed) {
      uniqueIds.add(trimmed);
    }
  }
  return Array.from(uniqueIds);
}

async function assignContactsToList(kvService, orgId, listId, contactIds = []) {
  if (!orgId || !listId) {
    return null;
  }

  const normalizedIds = normalizeContactIdsForList(contactIds);
  const listKey = `org:${orgId}:contactlist:${listId}`;
  const existingList = await kvService.get(listKey);

  if (!existingList) {
    console.warn('CONTACT LIST ASSIGNMENT - Contact list not found', { orgId, listId, totalContacts: normalizedIds.length });
    return null;
  }

  const updatedList = {
    ...existingList,
    contactIds: normalizedIds,
    contactCount: normalizedIds.length,
    updatedAt: new Date().toISOString()
  };

  await kvService.put(listKey, updatedList);
  return updatedList;
}

async function processSegmentBuildJob(message, kvService) {
  const { orgId, segmentId, filters = [], editMode, cursor: resumeCursor, chunkIndex: resumeChunkIndex = 0 } = message;
  const statusKey = `org:${orgId}:segment_job:${segmentId}`;
  const matchesKeyPrefix = `org:${orgId}:segment_job:${segmentId}:matches`;

  await kvService.setCache(statusKey, {
    status: 'processing',
    stage: 'loading_contacts',
    segmentId,
    editMode,
    cursor: resumeCursor || null,
    chunkIndex: resumeChunkIndex,
    updatedAt: new Date().toISOString()
  }, 7200);

  try {
    const segmentKey = `org:${orgId}:contactlist:${segmentId}`;
    const existingSegment = await kvService.get(segmentKey);

    if (!existingSegment) {
      throw new Error(`Segment ${segmentId} not found for org ${orgId}`);
    }

    const MAX_SAFE_OPS = kvService.maxOperations - 100;
    let cursor = resumeCursor || null;
    let chunkIndex = resumeChunkIndex;
    const contactMap = new Map();

    const CHUNK_LIMIT = 100;
    do {
      try {
        const list = await kvService.listMain({ prefix: `org:${orgId}:contact:`, limit: CHUNK_LIMIT, cursor });
        const keys = list.keys || [];
        const contacts = await fetchContactsForKeys(kvService, keys, contactMap);

        const matchingInChunk = (filters && filters.length > 0)
          ? contacts.filter(contact => evaluateSegmentFilters(contact, filters))
          : contacts;

        if (matchingInChunk.length > 0) {
          await kvService.setCache(`${matchesKeyPrefix}:${chunkIndex}`, matchingInChunk.map(contact => contact.id), 7200);
          chunkIndex++;
        }

        cursor = list.list_complete ? null : list.cursor;

        await kvService.setCache(statusKey, {
          status: 'processing',
          stage: cursor ? 'loading_contacts' : 'finalizing',
          segmentId,
          editMode,
          cursor,
          chunkIndex,
          updatedAt: new Date().toISOString()
        }, 7200);

        if (cursor && kvService.operationCount >= MAX_SAFE_OPS) {
          await kvService.setCache(statusKey, {
            status: 'queued',
            stage: 'rate_limited',
            segmentId,
            editMode,
            cursor,
            chunkIndex,
            updatedAt: new Date().toISOString()
          }, 7200);
          await enqueueSegmentContinuation(message, kvService, cursor, chunkIndex);
          return;
        }
      } catch (chunkError) {
        if (chunkError instanceof Error && chunkError.message.includes('KV rate limit')) {
          await kvService.setCache(statusKey, {
            status: 'queued',
            stage: 'rate_limited',
            segmentId,
            editMode,
            cursor,
            chunkIndex,
            updatedAt: new Date().toISOString()
          }, 7200);
          const continuationCursor = cursor || resumeCursor || null;
          await enqueueSegmentContinuation(message, kvService, continuationCursor, chunkIndex);
          return;
        }
        throw chunkError;
      }
    } while (cursor);

    const matchingContactIds = await collectSegmentMatches(kvService, matchesKeyPrefix, chunkIndex);
    const uniqueIds = Array.from(new Set(matchingContactIds));

    const updatedSegment = {
      ...existingSegment,
      contactIds: uniqueIds,
      contactCount: uniqueIds.length,
      lastRefreshed: new Date().toISOString(),
      status: 'ready'
    };

    await kvService.put(segmentKey, updatedSegment);

    const previousContactIds = existingSegment.contactIds || [];
    const toAdd = uniqueIds.filter(id => !previousContactIds.includes(id));
    const toRemove = previousContactIds.filter(id => !uniqueIds.includes(id));

    await updateContactsForSegment(kvService, orgId, contactMap, toAdd, segmentId, true);
    await updateContactsForSegment(kvService, orgId, contactMap, toRemove, segmentId, false);

    await kvService.setCache(statusKey, {
      status: 'completed',
      segmentId,
      matchedContacts: uniqueIds.length,
      completedAt: new Date().toISOString()
    }, 7200);
  } catch (error) {
    console.error('SEGMENT BUILD FAILED:', error);
    await kvService.setCache(statusKey, {
      status: 'failed',
      segmentId,
      error: error instanceof Error ? error.message : 'Segment build failed',
      failedAt: new Date().toISOString()
    }, 7200);
    throw error;
  }
}

async function fetchContactsForKeys(kvService, keys, contactMap) {
  const contacts = [];
  for (let i = 0; i < keys.length; i += 25) {
    const chunk = keys.slice(i, i + 25);
    const chunkResults = await Promise.all(chunk.map(async key => {
      const contact = await kvService.get(key.name);
      if (contact) {
        contactMap.set(contact.id, contact);
      }
      return contact;
    }));
    chunkResults.forEach(contact => {
      if (contact) {
        contacts.push(contact);
      }
    });
  }
  return contacts;
}

async function collectSegmentMatches(kvService, matchesKeyPrefix, chunkCount) {
  const ids = [];
  for (let i = 0; i < chunkCount; i++) {
    const chunk = await kvService.getCache(`${matchesKeyPrefix}:${i}`);
    if (Array.isArray(chunk)) {
      ids.push(...chunk);
    }
    await kvService.deleteCache(`${matchesKeyPrefix}:${i}`);
  }
  return ids;
}

async function enqueueSegmentContinuation(message, kvService, cursor, chunkIndex) {
  const queue = kvService.env?.CONTACT_PROCESSING_QUEUE;
  if (!queue?.send) {
    throw new Error('Queue binding missing for segment continuation');
  }
  await queue.send({
    ...message,
    cursor,
    chunkIndex,
    retryCount: (message.retryCount || 0) + 1
  });
}

async function updateContactsForSegment(kvService, orgId, contactMap, contactIds, segmentId, add = true) {
  if (!contactIds || contactIds.length === 0) {
    return;
  }

  const BATCH_SIZE = 25;
  for (let i = 0; i < contactIds.length; i += BATCH_SIZE) {
    const batch = contactIds.slice(i, i + BATCH_SIZE);
    await Promise.all(batch.map(async contactId => {
      let contact = contactMap.get(contactId);
      if (!contact) {
        contact = await kvService.get(`org:${orgId}:contact:${contactId}`);
        if (!contact) {
          return;
        }
      }

      const lists = Array.isArray(contact.contactListIds) ? contact.contactListIds : [];
      const hasSegment = lists.includes(segmentId);

      if (add && hasSegment) {
        return;
      }
      if (!add && !hasSegment) {
        return;
      }

      const updatedLists = add
        ? [...lists, segmentId]
        : lists.filter(id => id !== segmentId);

      const updatedContact = {
        ...contact,
        contactListIds: updatedLists,
        updatedAt: new Date().toISOString()
      };

      await kvService.put(`org:${orgId}:contact:${contactId}`, updatedContact);
      contactMap.set(contactId, updatedContact);
    }));
  }
}

function evaluateSegmentFilters(contact, filters) {
  if (!filters || filters.length === 0) {
    return true;
  }

  let result = evaluateSegmentRule(contact, filters[0]);
  for (let i = 1; i < filters.length; i++) {
    const ruleResult = evaluateSegmentRule(contact, filters[i]);
    if (filters[i].logic === 'OR') {
      result = result || ruleResult;
    } else {
      result = result && ruleResult;
    }
  }
  return result;
}

function evaluateSegmentRule(contact, rule) {
  const targetField = rule.field;
  let contactValue;

  if (['firstName', 'lastName', 'email', 'phone'].includes(targetField)) {
    contactValue = contact[targetField] || '';
  } else if (targetField === 'optedOut') {
    contactValue = contact.optedOut ? 'true' : 'false';
  } else {
    contactValue = contact.metadata?.[targetField] || '';
  }

  const contactStr = String(contactValue ?? '').toLowerCase();
  const filterStr = String(rule.value ?? '').toLowerCase();

  switch (rule.operator) {
    case 'equals':
      return contactStr === filterStr;
    case 'not_equals':
      return contactStr !== filterStr;
    case 'contains':
      return contactStr.includes(filterStr);
    case 'not_contains':
      return !contactStr.includes(filterStr);
    case 'starts_with':
      return contactStr.startsWith(filterStr);
    case 'ends_with':
      return contactStr.endsWith(filterStr);
    case 'greater_than':
      return parseFloat(contactValue) > parseFloat(rule.value);
    case 'less_than':
      return parseFloat(contactValue) < parseFloat(rule.value);
    case 'greater_equal':
      return parseFloat(contactValue) >= parseFloat(rule.value);
    case 'less_equal':
      return parseFloat(contactValue) <= parseFloat(rule.value);
    case 'is_empty':
      return !contactValue || contactValue === '';
    case 'is_not_empty':
      return !!contactValue && contactValue !== '';
    default:
      return false;
  }
}

async function checkAndFinalizeBatch(kvService, orgId, uploadId, listId, totalBatches) {
  try {
    // Check if all batches are completed
    const completedBatches = [];
    for (let i = 1; i <= totalBatches; i++) {
      const batchKey = `org:${orgId}:upload_batch:${uploadId}:${i}`;
      const batchStatus = await kvService.getCache(batchKey);
      if (batchStatus?.status === 'completed') {
        completedBatches.push(batchStatus);
      }
    }
    
    console.log(`📊 WORKER FINALIZE CHECK - Batches completed: ${completedBatches.length}/${totalBatches}`);
    
    if (completedBatches.length === totalBatches) {
      console.log('🎉 ALL BATCHES COMPLETED - Starting finalization');

      // Aggregate results
      const totalResults = completedBatches.reduce((acc, batch) => ({
        created: acc.created + (batch.created || 0),
        duplicatesUpdated: acc.duplicatesUpdated + (batch.duplicatesUpdated || 0),
        skippedDuplicates: acc.skippedDuplicates + (batch.skippedDuplicates || 0),
        errors: acc.errors + (batch.errors || 0),
        contacts: acc.contacts + (batch.contacts || 0)
      }), { created: 0, duplicatesUpdated: 0, skippedDuplicates: 0, errors: 0, contacts: 0 });

      const aggregatedContactIds = normalizeContactIdsForList(
        completedBatches.flatMap(batch => batch.contactIds || [])
      );

      if (aggregatedContactIds.length > 0) {
        await assignContactsToList(kvService, orgId, listId, aggregatedContactIds);
      } else {
        console.warn('CONTACT LIST ASSIGNMENT - No contact IDs found during finalization', { orgId, listId, uploadId });
      }

      const uploadKey = `org:${orgId}:upload_status:${uploadId}`;
      await kvService.setCache(uploadKey, {
        status: 'finalizing',
        stage: 'metadata_rebuild',
        processed: totalResults.contacts,
        total: totalResults.contacts,
        results: {
          listId,
          totalRows: totalResults.contacts,
          successfulRows: totalResults.created,
          duplicatesUpdated: totalResults.duplicatesUpdated,
          skippedDuplicates: totalResults.skippedDuplicates,
          failedRows: totalResults.errors,
          errors: []
        },
        updatedAt: new Date().toISOString()
      }, 7200);

      const finalizationKey = `org:${orgId}:upload_finalization:${uploadId}`;
      const jobId = await enqueueMetadataRebuild(kvService, {
        orgId,
        jobId: `upload:${uploadId}`,
        reason: 'contact_upload',
        uploadContext: {
          uploadId,
          listId,
          totalResults
        },
        finalizationKey
      });

      console.log('✅ WORKER FINALIZATION - Metadata rebuild queued', { orgId, jobId, uploadId });
    }
  } catch (error) {
    console.error('❌ WORKER FINALIZATION FAILED:', error);
  }
}

export default {
  async queue(batch, env, ctx) {
    const batchStartTime = Date.now();
    const queueTimestamp = batch.messages?.[0]?.timestamp || Date.now();
    const queueLatency = batchStartTime - queueTimestamp;

    console.log('🚀 QUEUE WORKER - Processing MessageBatch:', {
      batchKeys: Object.keys(batch),
      queueName: batch.queue,
      messageCount: batch.messages?.length || 0,
      queueLatencyMs: queueLatency
    });

    const kvService = new KVService(env);
    kvService.env = env; // Store env reference for contact service

    // Access messages from batch.messages (Cloudflare Queue format)
    const messages = batch.messages || [];

    // PHASE 1 OPTIMIZATION: Calculate total KV operations needed for entire batch upfront
    let totalEstimatedOps = 0;
    const messageMetrics = messages.map(message => {
      if (!message.body || !message.body.type) {
        return { message, estimatedOps: 0, valid: false };
      }

      let estimatedOps = 0;
      switch (message.body.type) {
        case MESSAGE_TYPES.CONTACT_BATCH:
          estimatedOps = (message.body.contacts?.length || 0) * 5;
          break;
        case MESSAGE_TYPES.CAMPAIGN_SEND:
          estimatedOps = (message.body.contacts?.length || 0) * 2;
          break;
        case MESSAGE_TYPES.CONTACT_DELETE:
          estimatedOps = (message.body.contactKeys?.length || 0);
          break;
        case MESSAGE_TYPES.OPT_OUT_BATCH:
          estimatedOps = (message.body.optOuts?.length || 0) * 3;
          break;
        case MESSAGE_TYPES.BULK_DELETE_ORG_DATA:
          // Bulk delete will enumerate keys dynamically, estimate conservatively
          estimatedOps = 500;
          break;
        case MESSAGE_TYPES.METADATA_REBUILD:
          estimatedOps = 900; // Force sequential processing
          break;
        case MESSAGE_TYPES.SEGMENT_BUILD:
          estimatedOps = 800;
          break;
        default:
          estimatedOps = 100;
      }

      totalEstimatedOps += estimatedOps;
      return {
        message,
        estimatedOps,
        valid: true,
        attempts: message.attempts || 0
      };
    });

    console.log(`📊 BATCH PRE-FLIGHT CHECK:`, {
      totalMessages: messages.length,
      totalEstimatedOps,
      kvLimit: kvService.maxOperations,
      withinLimit: totalEstimatedOps <= kvService.maxOperations
    });

    // PARALLEL PROCESSING: Process all messages in parallel if within KV limit
    const canProcessInParallel = totalEstimatedOps <= kvService.maxOperations;

    let processedCount = 0;
    let errorCount = 0;
    const messageTimings = [];

    if (canProcessInParallel) {
      console.log('⚡ PARALLEL MODE: Processing all messages concurrently');

      // Process all messages in parallel
      const results = await Promise.allSettled(
        messageMetrics.map(async ({ message, estimatedOps, valid, attempts }) => {
          const msgStartTime = Date.now();

          try {
            if (!valid) {
              console.error('❌ QUEUE WORKER - Message missing body or type:', message.id);
              message.retry();
              return { success: false, id: message.id, error: 'Invalid message', timing: 0 };
            }

            console.log(`📫 Processing ${message.body.type} message ${message.id} (attempt ${attempts + 1}, ~${estimatedOps} ops)`);

            // Route to appropriate processor
            switch (message.body.type) {
              case MESSAGE_TYPES.CONTACT_BATCH:
                await processContactBatch(message.body, kvService);
                break;
              case MESSAGE_TYPES.CAMPAIGN_SEND:
                await processCampaignBatch(message.body, kvService);
                break;
              case MESSAGE_TYPES.CONTACT_DELETE:
                await processContactDeletion(message.body, kvService);
                break;
              case MESSAGE_TYPES.OPT_OUT_BATCH:
                await processOptOutBatch(message.body, kvService);
                break;
              case MESSAGE_TYPES.BULK_DELETE_ORG_DATA:
                await processBulkDeleteOrgData(message.body, kvService);
                break;
              case MESSAGE_TYPES.METADATA_REBUILD:
                await processMetadataRebuildJob(message.body, kvService);
                break;
              case MESSAGE_TYPES.SEGMENT_BUILD:
                await processSegmentBuildJob(message.body, kvService);
                break;
              default:
                throw new Error(`Unknown message type: ${message.body.type}`);
            }

            const msgTiming = Date.now() - msgStartTime;
            message.ack();

            console.log(`✅ Message ${message.id} (${message.body.type}) completed in ${msgTiming}ms`);

            return {
              success: true,
              id: message.id,
              type: message.body.type,
              timing: msgTiming,
              attempts,
              estimatedOps
            };

          } catch (error) {
            const msgTiming = Date.now() - msgStartTime;
            console.error(`❌ QUEUE WORKER - Failed to process message ${message.id}:`, error);
            message.retry();

            return {
              success: false,
              id: message.id,
              error: error.message,
              timing: msgTiming,
              attempts
            };
          }
        })
      );

      // Process results
      results.forEach(result => {
        if (result.status === 'fulfilled') {
          if (result.value.success) {
            processedCount++;
            messageTimings.push(result.value);
          } else {
            errorCount++;
          }
        } else {
          errorCount++;
        }
      });

    } else {
      // SEQUENTIAL FALLBACK: Process messages one at a time until KV limit
      console.log('⚠️ SEQUENTIAL MODE: Batch exceeds KV limit, processing sequentially');

      for (const { message, estimatedOps, valid, attempts } of messageMetrics) {
        const msgStartTime = Date.now();

        try {
          if (!valid) {
            console.error('❌ QUEUE WORKER - Message missing body or type:', message.id);
            message.retry();
            errorCount++;
            continue;
          }

          // Check if we have enough KV operations left
          if (kvService.operationCount + estimatedOps > kvService.maxOperations) {
            console.warn(`⚠️ QUEUE WORKER - KV limit reached, retrying message ${message.id}`);
            message.retry();
            errorCount++;
            continue;
          }

          console.log(`📫 Processing ${message.body.type} message ${message.id} (attempt ${attempts + 1})`);

          // Route to appropriate processor
          switch (message.body.type) {
            case MESSAGE_TYPES.CONTACT_BATCH:
              await processContactBatch(message.body, kvService);
              break;
            case MESSAGE_TYPES.CAMPAIGN_SEND:
              await processCampaignBatch(message.body, kvService);
              break;
            case MESSAGE_TYPES.CONTACT_DELETE:
              await processContactDeletion(message.body, kvService);
              break;
            case MESSAGE_TYPES.OPT_OUT_BATCH:
              await processOptOutBatch(message.body, kvService);
              break;
            case MESSAGE_TYPES.BULK_DELETE_ORG_DATA:
              await processBulkDeleteOrgData(message.body, kvService);
              break;
            case MESSAGE_TYPES.METADATA_REBUILD:
              await processMetadataRebuildJob(message.body, kvService);
              break;
            case MESSAGE_TYPES.SEGMENT_BUILD:
              await processSegmentBuildJob(message.body, kvService);
              break;
            default:
              throw new Error(`Unknown message type: ${message.body.type}`);
          }

          const msgTiming = Date.now() - msgStartTime;
          message.ack();
          processedCount++;

          messageTimings.push({
            success: true,
            id: message.id,
            type: message.body.type,
            timing: msgTiming,
            attempts,
            estimatedOps
          });

          console.log(`✅ Message ${message.id} completed in ${msgTiming}ms - KV ops: ${kvService.operationCount}/${kvService.maxOperations}`);

        } catch (error) {
          const msgTiming = Date.now() - msgStartTime;
          console.error(`❌ QUEUE WORKER - Failed to process message ${message.id}:`, error);
          message.retry();
          errorCount++;
        }
      }
    }

    const batchTotalTime = Date.now() - batchStartTime;
    const avgMessageTime = messageTimings.length > 0
      ? messageTimings.reduce((sum, m) => sum + m.timing, 0) / messageTimings.length
      : 0;

    // TELEMETRY: Store metrics in analytics KV
    const telemetryData = {
      timestamp: new Date().toISOString(),
      batchId: batch.queue + '_' + batchStartTime,
      queueName: batch.queue,
      processingMode: canProcessInParallel ? 'parallel' : 'sequential',
      metrics: {
        totalMessages: messages.length,
        processedMessages: processedCount,
        errorMessages: errorCount,
        queueLatencyMs: queueLatency,
        batchProcessingTimeMs: batchTotalTime,
        avgMessageTimeMs: Math.round(avgMessageTime),
        kvOperationsUsed: kvService.operationCount,
        kvOperationsLimit: kvService.maxOperations,
        kvUtilization: Math.round((kvService.operationCount / kvService.maxOperations) * 100) + '%'
      },
      messageBreakdown: messageTimings.map(m => ({
        type: m.type,
        timingMs: m.timing,
        attempts: m.attempts,
        estimatedOps: m.estimatedOps
      }))
    };

    // Store telemetry
    try {
      const telemetryKey = `metrics:queue:batch:${Date.now()}`;
      await kvService.analytics.put(telemetryKey, JSON.stringify(telemetryData), {
        expirationTtl: 604800 // 7 days
      });
    } catch (error) {
      console.error('Failed to store telemetry:', error);
    }

    console.log(`✅ QUEUE WORKER - Batch completed:`, {
      mode: canProcessInParallel ? 'PARALLEL' : 'SEQUENTIAL',
      totalMessages: messages.length,
      processed: processedCount,
      errors: errorCount,
      queueLatencyMs: queueLatency,
      batchTimeMs: batchTotalTime,
      avgMsgTimeMs: Math.round(avgMessageTime),
      kvOpsUsed: kvService.operationCount,
      kvOpsRemaining: kvService.maxOperations - kvService.operationCount,
      kvUtilization: Math.round((kvService.operationCount / kvService.maxOperations) * 100) + '%'
    });

    // Don't throw errors - we handled everything individually
  }
};
