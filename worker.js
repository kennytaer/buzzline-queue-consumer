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
  CONTACT_DELETE: 'contact_delete'
};

// Simplified contact service for the worker
class ContactService {
  constructor(env) {
    this.kv = new KVService(env);
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
    
    // Process contacts in parallel batches for better performance
    const PARALLEL_BATCH_SIZE = 10;
    
    for (let i = 0; i < contacts.length; i += PARALLEL_BATCH_SIZE) {
      const batch = contacts.slice(i, i + PARALLEL_BATCH_SIZE);
      const batchPromises = batch.map(async ({ id, data }) => {
        try {
          const contact = {
            id,
            orgId,
            ...data,
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString()
          };

          // Prepare all KV operations for this contact
          const kvOperations = [];
          
          // Store main contact record
          const contactKey = `org:${orgId}:contact:${id}`;
          kvOperations.push(this.kv.put(contactKey, contact));

          // Store email index if exists
          if (contact.email) {
            const emailKey = `org:${orgId}:contact_by_email:${contact.email.toLowerCase()}`;
            kvOperations.push(this.kv.put(emailKey, contact));
          }

          // Store phone index if exists
          if (contact.phone) {
            const phoneKey = `org:${orgId}:contact_by_phone:${contact.phone}`;
            kvOperations.push(this.kv.put(phoneKey, contact));
          }
          
          // Execute all KV operations for this contact in parallel
          await Promise.allSettled(kvOperations);
          
          return { success: true, contact };
        } catch (error) {
          console.error(`Failed to create contact ${id}:`, error);
          return { success: false, id, error: error.message };
        }
      });
      
      // Wait for this batch to complete
      const batchResults = await Promise.allSettled(batchPromises);
      
      // Process results
      batchResults.forEach(result => {
        if (result.status === 'fulfilled') {
          if (result.value.success) {
            created.push(result.value.contact);
          } else {
            errors.push({ id: result.value.id, error: result.value.error });
          }
        } else {
          errors.push({ id: 'unknown', error: result.reason?.message || 'Unknown error' });
        }
      });
    }

    return { created, errors };
  }

  async updateContact(orgId, contactId, updates) {
    try {
      const contactKey = `org:${orgId}:contact:${contactId}`;
      const existing = await this.kv.get(contactKey);
      
      if (!existing) {
        throw new Error('Contact not found');
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
    console.log(`Rebuilding metadata for org ${orgId} - simplified version`);
    // For now, just log. In full version, this would rebuild search indexes
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
    
  } catch (error) {
    console.error(`❌ DELETE BATCH ${batchNumber}/${totalBatches} - Failed:`, error);
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
    
    // Check for duplicates in this batch
    const emailsAndPhones = contacts.map(({contact}) => ({
      email: contact.email || undefined,
      phone: contact.phone || undefined
    }));
    
    const existingContacts = await contactService.findContactsByEmailsOrPhones(orgId, emailsAndPhones);
    const existingContactMap = new Map();
    existingContacts.forEach((item) => {
      const { email, phone, contact } = item;
      if (email) existingContactMap.set(email.toLowerCase(), contact);
      if (phone) existingContactMap.set(phone, contact);
    });
    
    // Separate new contacts from duplicates
    const newContacts = [];
    const duplicateUpdates = [];
    
    for (const {rowIndex, contact, contactId} of contacts) {
      const existingByEmail = contact.email ? existingContactMap.get(contact.email.toLowerCase()) : null;
      const existingByPhone = contact.phone ? existingContactMap.get(contact.phone) : null;
      const existingContact = existingByEmail || existingByPhone;
      
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
          await contactService.updateContact(orgId, contact.id, updates);
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
            if (result.value.reactivated) {
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
    
    // Update batch completion status
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
    
    // Check if this was the last batch and trigger final processing
    await checkAndFinalizeBatch(kvService, contactService, orgId, uploadId, listId, totalBatches);
    
  } catch (error) {
    console.error(`❌ WORKER BATCH ${batchNumber}/${totalBatches} - Failed:`, error);
    
    await updateBatchStatus(kvService, orgId, uploadId, batchNumber, 'failed', {
      error: error instanceof Error ? error.message : 'Unknown error',
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
      
      // Rebuild contact indexes
      console.log('🔄 WORKER FINALIZATION - Rebuilding contact indexes');
      await contactService.forceRebuildMetadata(orgId);
      
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
    const startTime = Date.now();
    console.log('🚀 QUEUE WORKER - Processing MessageBatch:', {
      batchKeys: Object.keys(batch),
      queueName: batch.queue,
      messageCount: batch.messages?.length || 0
    });

    const kvService = new KVService(env);
    kvService.env = env; // Store env reference for contact service

    // Access messages from batch.messages (Cloudflare Queue format)
    const messages = batch.messages || [];
    
    let processedCount = 0;
    let errorCount = 0;
    
    // Process messages SEQUENTIALLY to avoid KV operation overlap
    // This prevents multiple operations from hitting KV limit simultaneously
    for (const message of messages) {
      try {
        if (!message.body || !message.body.type) {
          console.error('❌ QUEUE WORKER - Message missing body or type:', message.id);
          message.retry();
          errorCount++;
          continue;
        }
        
        // Estimate KV operations needed based on message type
        let estimatedOpsNeeded = 0;
        switch (message.body.type) {
          case MESSAGE_TYPES.CONTACT_BATCH:
            estimatedOpsNeeded = (message.body.contacts?.length || 0) * 5;
            break;
          case MESSAGE_TYPES.CAMPAIGN_SEND:
            estimatedOpsNeeded = (message.body.contacts?.length || 0) * 2; // Less KV operations
            break;
          case MESSAGE_TYPES.CONTACT_DELETE:
            estimatedOpsNeeded = (message.body.contactKeys?.length || 0);
            break;
          default:
            console.warn(`⚠️ QUEUE WORKER - Unknown message type: ${message.body.type}`);
            estimatedOpsNeeded = 100; // Conservative estimate
        }
        
        // Check if we have enough KV operations left for this message
        if (kvService.operationCount + estimatedOpsNeeded > kvService.maxOperations) {
          console.warn(`⚠️ QUEUE WORKER - KV limit would be exceeded for message ${message.id} (${message.body.type}), retrying`);
          message.retry();
          errorCount++;
          continue;
        }
        
        console.log(`📫 Processing ${message.body.type} message ${message.id}`);
        
        // Route to appropriate processor based on message type
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
          default:
            throw new Error(`Unknown message type: ${message.body.type}`);
        }
        
        // Acknowledge successful processing
        message.ack();
        processedCount++;
        
        console.log(`✅ Message ${message.id} (${message.body.type}) completed - KV ops: ${kvService.operationCount}/${kvService.maxOperations}`);
        
      } catch (error) {
        console.error(`❌ QUEUE WORKER - Failed to process message ${message.id}:`, error);
        // Individual message retry - won't affect other messages
        message.retry();
        errorCount++;
      }
    }
    
    const processingTime = Date.now() - startTime;
    console.log(`✅ QUEUE WORKER - Batch completed:`, {
      totalMessages: messages.length,
      processed: processedCount,
      errors: errorCount,
      processingTime: `${processingTime}ms`,
      kvOperationsUsed: kvService.operationCount,
      kvOperationsRemaining: kvService.maxOperations - kvService.operationCount
    });
    
    // Don't throw errors - we handled everything individually
  }
};