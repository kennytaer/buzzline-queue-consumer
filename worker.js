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
}

// Message types for unified queue processing
const MESSAGE_TYPES = {
  CONTACT_BATCH: 'contact_batch',
  CAMPAIGN_SEND: 'campaign_send',
  CONTACT_DELETE: 'contact_delete',
  OPT_OUT_BATCH: 'opt_out_batch',
  BULK_DELETE_ORG_DATA: 'bulk_delete_org_data'
};

// Contact metadata management
class ContactMetadataService {
  constructor(kvService) {
    this.kv = kvService;
  }
  
  // Get or initialize contact metadata for an organization
  async getContactMetadata(orgId) {
    const metaKey = `org:${orgId}:contact_metadata`;
    const metadata = await this.kv.get(metaKey);
    
    if (metadata) {
      return metadata;
    }
    
    // Initialize metadata if it doesn't exist
    const initialMetadata = {
      totalContacts: 0,
      contactsWithEmail: 0,
      contactsWithPhone: 0,
      contactsWithBoth: 0,
      subscribedContacts: 0,
      optedOutContacts: 0,
      lastUpdated: new Date().toISOString(),
      version: 1
    };
    
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
    const metadata = {
      totalContacts: 0,
      contactsWithEmail: 0,
      contactsWithPhone: 0,
      contactsWithBoth: 0,
      subscribedContacts: 0,
      optedOutContacts: 0,
      lastUpdated: new Date().toISOString(),
      version: 1
    };

    let cursor = undefined;
    let batchNumber = 0;

    do {
      batchNumber++;
      // Use smaller batch size to avoid rate limits (Workers: 1000 reads/sec limit)
      const list = await this.kv.main.list({ prefix, limit: 500, cursor });

      console.log(`📊 Metadata rebuild batch ${batchNumber}: ${list.keys.length} contacts`);

      // Process contacts in smaller chunks to avoid rate limits
      const CHUNK_SIZE = 100;
      const allContacts = [];

      for (let i = 0; i < list.keys.length; i += CHUNK_SIZE) {
        const chunk = list.keys.slice(i, i + CHUNK_SIZE);
        const chunkPromises = chunk.map(async (key) => {
          try {
            const contactData = await this.kv.main.get(key.name);
            return contactData ? JSON.parse(contactData) : null;
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
      const existingMeta = await this.kv.cache.get(metaKey);
      if (existingMeta) {
        const metadata = JSON.parse(existingMeta);
        console.log(`🧹 WORKER - Clearing ${metadata.totalPages || 0} cached pages`);

        // Clear all cached page indexes
        for (let page = 1; page <= (metadata.totalPages || 0) + 1; page++) {
          const pageKey = `org:${orgId}:contact_index:page_${page}`;
          await this.kv.cache.delete(pageKey);
        }
      }

      // Delete metadata and search index
      await this.kv.cache.delete(metaKey);
      await this.kv.cache.delete(searchKey);

      console.log('✅ WORKER - Cache invalidated. Dashboard will rebuild on next load.');

      return { invalidated: true, message: 'Cache cleared for lazy rebuild' };
    } catch (error) {
      console.error('❌ WORKER - Cache invalidation failed:', error);
      throw error;
    }
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
    
    // Process contacts in smaller batches to avoid overwhelming external APIs
    const SEND_BATCH_SIZE = 5; // Very conservative for API limits
    
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
              sendEmail(contact, campaign, salesMember, messagingEndpoints.email)
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
        await new Promise(resolve => setTimeout(resolve, 2000)); // 2 second delay
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
    await checkAndFinalizeOptOutBatches(kvService, contactService, orgId, totalBatches);

  } catch (error) {
    console.error(`❌ OPT-OUT BATCH ${batchNumber}/${totalBatches} - Failed:`, error);
    throw error;
  }
}

// Check and finalize opt-out batches
async function checkAndFinalizeOptOutBatches(kvService, contactService, orgId, totalBatches) {
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

      // Rebuild metadata to ensure accurate counts (fixes race condition)
      console.log('🔄 OPT-OUT FINALIZATION - Rebuilding contact metadata...');
      try {
        await contactService.metadataService.rebuildContactMetadata(orgId);
        console.log('✅ OPT-OUT FINALIZATION - Contact metadata rebuilt successfully');
      } catch (metadataError) {
        console.error('❌ OPT-OUT FINALIZATION - Failed to rebuild metadata:', metadataError);
      }

      // Invalidate dashboard cache
      try {
        await contactService.forceRebuildMetadata(orgId);
        console.log('✅ OPT-OUT FINALIZATION - Dashboard cache invalidated');
      } catch (cacheError) {
        console.error('❌ OPT-OUT FINALIZATION - Failed to invalidate cache:', cacheError);
      }

      console.log('✅ OPT-OUT FINALIZATION COMPLETE');
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
    const contactService = new ContactService(kvService.env);
    await checkAndFinalizeContactDeletion(kvService, contactService, orgId, totalBatches);

  } catch (error) {
    console.error(`❌ DELETE BATCH ${batchNumber}/${totalBatches} - Failed:`, error);
    throw error;
  }
}

// Check and finalize contact deletion batches
async function checkAndFinalizeContactDeletion(kvService, contactService, orgId, totalBatches) {
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

      // Rebuild metadata to ensure accurate counts (fixes race condition)
      console.log('🔄 DELETION FINALIZATION - Rebuilding contact metadata...');
      try {
        await contactService.metadataService.rebuildContactMetadata(orgId);
        console.log('✅ DELETION FINALIZATION - Contact metadata rebuilt successfully');
      } catch (metadataError) {
        console.error('❌ DELETION FINALIZATION - Failed to rebuild metadata:', metadataError);
      }

      // Invalidate dashboard cache
      try {
        await contactService.forceRebuildMetadata(orgId);
        console.log('✅ DELETION FINALIZATION - Dashboard cache invalidated');
      } catch (cacheError) {
        console.error('❌ DELETION FINALIZATION - Failed to invalidate cache:', cacheError);
      }

      console.log('✅ DELETION FINALIZATION COMPLETE');
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
    const contactService = new ContactService(kvService.env);
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
        startedAt: new Date().toISOString()
      };
    }

    console.log(`🗑️ BULK DELETE - Progress: ${progress.processedPrefixes.length}/${prefixesToDelete.length} prefixes complete, ${progress.totalDeleted} keys deleted`);

    const MAX_DELETES_PER_INVOCATION = 100; // Conservative limit
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

      // Check if we're approaching KV limits
      const remainingOps = kvService.maxOperations - kvService.operationCount;
      if (remainingOps < 50 || deletedThisInvocation >= MAX_DELETES_PER_INVOCATION) {
        console.warn(`⚠️ BULK DELETE - Approaching limits (remainingOps: ${remainingOps}, deleted: ${deletedThisInvocation}), will continue in next invocation`);
        needsContinuation = true;
        break; // Don't process more prefixes, queue continuation
      }

      // List a small batch of keys
      const list = await kvService.main.list({ prefix, limit: 100 });
      console.log(`📋 BULK DELETE - Found ${list.keys.length} keys for prefix: ${prefix} (list_complete: ${list.list_complete})`);

      if (list.keys.length === 0 && list.list_complete) {
        // No keys found, mark prefix as complete
        progress.processedPrefixes.push(prefix);
        console.log(`✅ BULK DELETE - Prefix complete (no keys found): ${prefix}`);
        continue;
      }

      // Delete in batches of 20
      const DELETE_BATCH_SIZE = 20;
      for (let i = 0; i < list.keys.length; i += DELETE_BATCH_SIZE) {
        const batch = list.keys.slice(i, i + DELETE_BATCH_SIZE);
        await Promise.all(batch.map(key => kvService.main.delete(key.name)));
        deletedThisInvocation += batch.length;
        progress.totalDeleted += batch.length;
      }

      console.log(`✅ BULK DELETE - Deleted ${list.keys.length} keys for prefix: ${prefix} (total: ${progress.totalDeleted})`);

      // If this prefix is complete, mark it as processed
      if (list.list_complete) {
        progress.processedPrefixes.push(prefix);
        console.log(`✅ BULK DELETE - Prefix complete: ${prefix}`);
      } else {
        // More keys remain for this prefix - need to continue
        console.log(`🔄 BULK DELETE - More keys remain for prefix: ${prefix}, will continue in next invocation`);
        needsContinuation = true;
        break; // Stop processing, queue continuation
      }
    }

    // Save progress
    await kvService.setCache(progressKey, progress, 3600);

    // Check if all prefixes are processed
    if (progress.processedPrefixes.length === prefixesToDelete.length) {
      // All prefixes complete - rebuild metadata and clean up progress
      console.log('🔄 BULK DELETE FINALIZATION - Rebuilding contact metadata...');
      try {
        await contactService.metadataService.rebuildContactMetadata(orgId);
        console.log('✅ BULK DELETE FINALIZATION - Contact metadata rebuilt successfully');
      } catch (metadataError) {
        console.error('❌ BULK DELETE FINALIZATION - Failed to rebuild metadata:', metadataError);
      }

      // Invalidate dashboard cache
      try {
        await contactService.forceRebuildMetadata(orgId);
        console.log('✅ BULK DELETE FINALIZATION - Dashboard cache invalidated');
      } catch (cacheError) {
        console.error('❌ BULK DELETE FINALIZATION - Failed to invalidate cache:', cacheError);
      }

      // Delete progress tracker
      await kvService.cache.delete(progressKey);

      console.log(`✅ BULK DELETE ORG DATA COMPLETE - Deleted ${progress.totalDeleted} keys for org: ${orgId}`);
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

    // PHASE 3 REVISED: Parallel duplicate checking using direct KV lookups
    console.log(`📊 PHASE 3 - Checking duplicates using parallel KV lookups`);

    // Build all KV lookup promises in parallel
    const duplicateCheckPromises = contacts.map(async ({rowIndex, contact, contactId}) => {
      let existingContact = null;

      // Check email index (parallel)
      if (contact.email) {
        const emailKey = `org:${orgId}:contact_by_email:${contact.email.toLowerCase()}`;
        const emailContact = await contactService.kv.get(emailKey);
        if (emailContact) {
          existingContact = emailContact;
        }
      }

      // Check phone index if no email match (only if needed)
      if (!existingContact && contact.phone) {
        const phoneKey = `org:${orgId}:contact_by_phone:${contact.phone}`;
        const phoneContact = await contactService.kv.get(phoneKey);
        if (phoneContact) {
          existingContact = phoneContact;
        }
      }

      return {
        rowIndex,
        contact,
        contactId,
        existingContact
      };
    });

    // Execute all duplicate checks in parallel
    const duplicateCheckResults = await Promise.all(duplicateCheckPromises);

    // Separate new contacts from duplicates based on results
    const newContacts = [];
    const duplicateUpdates = [];

    for (const {rowIndex, contact, contactId, existingContact} of duplicateCheckResults) {
      if (existingContact) {
        // Handle duplicate
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

        duplicateUpdates.push({contact: existingContact, updates});
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
    }
    
    // Process duplicate updates in parallel batches
    let duplicatesUpdated = 0;
    let skippedDuplicates = 0;
    
    const DUPLICATE_BATCH_SIZE = 5; // Smaller batches for updates
    
    for (let i = 0; i < duplicateUpdates.length; i += DUPLICATE_BATCH_SIZE) {
      const batch = duplicateUpdates.slice(i, i + DUPLICATE_BATCH_SIZE);
      
      const updatePromises = batch.map(async ({contact, updates}) => {
        try {
          const result = await contactService.updateContact(orgId, contact.id, updates);
          // If result is null, contact wasn't found after retries (race condition)
          if (result === null) {
            console.warn(`⚠️ Skipping update for contact ${contact.id} - not found after retries`);
            return { success: true, skipped: true };
          }
          return {
            success: true,
            reactivated: reactivateDuplicates && contact.optedOut
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
    
    // ALWAYS write completion status so finalization can track progress accurately
    await updateBatchStatus(kvService, orgId, uploadId, batchNumber, 'completed', {
      contacts: contacts.length,
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
    await checkAndFinalizeBatch(kvService, contactService, orgId, uploadId, listId, totalBatches);
    
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

async function checkAndFinalizeBatch(kvService, contactService, orgId, uploadId, listId, totalBatches) {
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

      // CRITICAL FIX: Rebuild contact metadata to fix race condition in incremental updates
      // Incremental updates from parallel batches suffer from read-modify-write race conditions
      // This ensures accurate counts by scanning all contacts (now with rate limiting)
      console.log('🔄 WORKER FINALIZATION - Rebuilding contact metadata for accurate counts...');
      try {
        await contactService.metadataService.rebuildContactMetadata(orgId);
        console.log('✅ WORKER FINALIZATION - Contact metadata rebuilt successfully');
      } catch (metadataError) {
        console.error('❌ WORKER FINALIZATION - Failed to rebuild metadata:', metadataError);
        // Don't fail the entire upload for metadata rebuild issues
      }

      // Invalidate dashboard cache so it rebuilds with fresh data
      try {
        await contactService.forceRebuildMetadata(orgId);
        console.log('✅ WORKER FINALIZATION - Dashboard cache invalidated');
      } catch (cacheError) {
        console.error('❌ WORKER FINALIZATION - Failed to invalidate cache:', cacheError);
      }

      // Update final upload status
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

      console.log('✅ WORKER FINALIZATION COMPLETE - Upload finished:', totalResults);
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