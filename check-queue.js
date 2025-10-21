// Quick diagnostic script to check queue status
// Run with: node check-queue.js

console.log(`
📊 QUEUE DIAGNOSTIC CHECKLIST

Stalled at: 1456/7256 contacts (20% complete)
Expected batches: ~145 (7256 / 50 contacts per batch)
Processed batches: ~29 (1456 / 50)

POSSIBLE ISSUES:

1. ❌ Messages hit max_retries (5) and moved to DLQ
   - Check Cloudflare Queue dashboard for failed messages
   - Look for "Dead Letter Queue" or retry count

2. ❌ Worker exceptions not being caught
   - Unhandled promise rejections
   - JavaScript errors in worker.js

3. ❌ KV rate limit exceeded
   - Even with 950 op buffer, parallel batches might exceed
   - Check if worker logs show "KV rate limit reached"

4. ❌ Message size too large (128 KB limit)
   - Each message has 50 contacts + metadata
   - If contacts have large custom fields, might exceed limit

5. ❌ Worker CPU time limit (30s on free plan, 30-50ms typical)
   - Large batches with retries might timeout

IMMEDIATE FIXES TO TRY:

A. Reduce batch size in upload handler
   - Change from 50 to 25 contacts per message
   - File: Buzzline/app/routes/dashboard.contacts.upload.tsx

B. Add explicit error boundaries
   - Wrap entire worker handler in try/catch
   - Log ALL errors before retrying

C. Check Cloudflare dashboard:
   - Go to Workers & Pages → buzzline-queue-consumer
   - Click on "Metrics" tab
   - Look for "Errors" and "CPU Time"
   - Check Queue dashboard for message backlog

D. Manually drain/purge queue and re-upload smaller test file
   - Upload just 100 contacts to test if system recovers
`);
