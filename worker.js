// Queue consumer worker for BuzzLine - Self-contained version

// Simplified KV service for the worker
class KVService {
  constructor(env) {
    this.main = env.BUZZLINE_MAIN;
    this.cache = env.BUZZLINE_CACHE;
    this.analytics = env.BUZZLINE_ANALYTICS;
  }

  async getCache(key) {
    try {
      const result = await this.cache.get(key);
      return result ? JSON.parse(result) : null;
    } catch (error) {
      console.error('KV get error:', error);
      return null;
    }
  }

  async setCache(key, value, ttl = 3600) {
    try {
      await this.cache.put(key, JSON.stringify(value), { expirationTtl: ttl });
    } catch (error) {
      console.error('KV set error:', error);
    }
  }

  async get(key) {
    try {
      const result = await this.main.get(key);
      return result ? JSON.parse(result) : null;
    } catch (error) {
      console.error('KV main get error:', error);
      return null;
    }
  }

  async put(key, value) {
    try {
      await this.main.put(key, JSON.stringify(value));
    } catch (error) {
      console.error('KV main put error:', error);
    }
  }
}

// Simplified contact service for the worker
class ContactService {
  constructor(env) {
    this.kv = new KVService(env);
  }

  async findContactsByEmailsOrPhones(orgId, emailsAndPhones) {
    try {
      const contacts = [];
      for (const { email, phone } of emailsAndPhones) {
        if (email) {
          const emailKey = `org:${orgId}:contact_by_email:${email.toLowerCase()}`;
          const contact = await this.kv.get(emailKey);
          if (contact) contacts.push({ email, contact });
        }
        if (phone) {
          const phoneKey = `org:${orgId}:contact_by_phone:${phone}`;
          const contact = await this.kv.get(phoneKey);
          if (contact) contacts.push({ phone, contact });
        }
      }
      return contacts;
    } catch (error) {
      console.error('Error finding contacts:', error);
      return [];
    }
  }

  async createContactsBulk(orgId, contacts) {
    const created = [];
    const errors = [];

    for (const { id, data } of contacts) {
      try {
        const contact = {
          id,
          orgId,
          ...data,
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString()
        };

        // Store main contact record
        const contactKey = `org:${orgId}:contact:${id}`;
        await this.kv.put(contactKey, contact);

        // Store email index if exists
        if (contact.email) {
          const emailKey = `org:${orgId}:contact_by_email:${contact.email.toLowerCase()}`;
          await this.kv.put(emailKey, contact);
        }

        // Store phone index if exists
        if (contact.phone) {
          const phoneKey = `org:${orgId}:contact_by_phone:${contact.phone}`;
          await this.kv.put(phoneKey, contact);
        }

        created.push(contact);
      } catch (error) {
        console.error(`Failed to create contact ${id}:`, error);
        errors.push({ id, error: error.message });
      }
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

      await this.kv.put(contactKey, updated);

      // Update indexes if email/phone changed
      if (updates.email && updates.email !== existing.email) {
        const emailKey = `org:${orgId}:contact_by_email:${updates.email.toLowerCase()}`;
        await this.kv.put(emailKey, updated);
      }

      if (updates.phone && updates.phone !== existing.phone) {
        const phoneKey = `org:${orgId}:contact_by_phone:${updates.phone}`;
        await this.kv.put(phoneKey, updated);
      }

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

async function processContactBatch(message, kvService) {
  const { uploadId, orgId, listId, listName, batchNumber, totalBatches, contacts, reactivateDuplicates } = message;
  
  console.log(`📦 WORKER BATCH ${batchNumber}/${totalBatches} - Processing ${contacts.length} contacts for upload ${uploadId}`);
  
  try {
    const contactService = new ContactService(kvService.env);
    
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
    
    // Process duplicate updates  
    let duplicatesUpdated = 0;
    let skippedDuplicates = 0;
    
    for (const {contact, updates} of duplicateUpdates) {
      try {
        await contactService.updateContact(orgId, contact.id, updates);
        if (reactivateDuplicates && contact.optedOut) {
          duplicatesUpdated++;
        } else {
          skippedDuplicates++;
        }
      } catch (error) {
        console.error(`Failed to update duplicate contact ${contact.id}:`, error);
        results.errors.push({ id: contact.id, error: "Failed to update duplicate" });
      }
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
      errors: results.errors.length
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
    console.log('🚀 QUEUE WORKER - Processing MessageBatch:', {
      batchKeys: Object.keys(batch),
      queueName: batch.queue,
      messageCount: batch.messages?.length || 0
    });

    const kvService = new KVService(env);
    kvService.env = env; // Store env reference for contact service

    // Access messages from batch.messages (Cloudflare Queue format)
    const messages = batch.messages || [];
    
    for (const message of messages) {
      try {
        if (message.body) {
          await processContactBatch(message.body, kvService);
          // Acknowledge successful processing
          message.ack();
        } else {
          console.error('❌ QUEUE WORKER - Message missing body:', message);
          message.retry();
        }
      } catch (error) {
        console.error(`❌ QUEUE WORKER - Failed to process message ${message.id}:`, error);
        message.retry();
      }
    }
  }
};