/**
 * Email Monitoring Service (v2.0)
 *
 * Monitors IMAP mailboxes for incoming emails with attachments,
 * saves PDFs/XLSX/ZIPs to workspace, and emits events for
 * downstream processing (classification, pipeline, email sending).
 *
 * Architecture:
 *  - EmailMonitor: one per client, owns IMAP connection + poll loop
 *  - EmailService: singleton manager, forwards events from all monitors
 */

import Imap from 'imap';
import { simpleParser } from 'mailparser';
import nodemailer from 'nodemailer';
import path from 'path';
import fs from 'fs';
import net from 'net';
import extractZip from 'extract-zip';
import { EventEmitter } from 'events';

import { baseDir, workspacePath, dataPath } from '../utils/paths';
import {
  recordProcessedEmail,
  updateProcessedEmail,
} from '../stores/client.store';
import type {
  ClientSettings,
  ProcessedEmail,
  EmailServiceStatus,
  IncomingEmail,
} from '../../shared/types';

// Re-export mailparser for callers that need direct parsing
export { simpleParser };

// ---------------------------------------------------------------------------
// Extended runtime types (not serialisable over IPC — kept here, not shared/)
// ---------------------------------------------------------------------------

/** Full email with raw attachment buffers — only used on the main process side. */
export interface RuntimeEmail {
  messageId: string;
  uid: number;
  subject: string;
  from: string;
  to: string;
  date: Date;
  body: string;
  attachments: EmailAttachment[];
}

export interface EmailAttachment {
  filename: string;
  contentType: string;
  content: Buffer;
  size: number;
}

export interface SendEmailOptions {
  to: string | string[];
  subject: string;
  body: string;
  attachments?: { filename: string; path: string }[];
}

// ---------------------------------------------------------------------------
// File-based logger
// ---------------------------------------------------------------------------

const TAG = '[email-service]';

function emailLog(msg: string): void {
  const ts = new Date().toISOString();
  const line = `[${ts}] ${msg}\n`;
  try {
    const logPath = path.join(dataPath(), 'email-service.log');
    fs.appendFileSync(logPath, line);
  } catch {
    // dataPath() may throw if baseDir not yet initialised
  }
  console.log(`${TAG} ${msg}`);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Sanitise a string for use as a filesystem name. */
function sanitiseFilename(input: string, maxLen = 120): string {
  return (input || 'untitled')
    .replace(/[<>:"/\\|?*]/g, '_')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, maxLen);
}

/** Derive workspace prefix from email address (e.g. "shipments.co@..." -> "shipments"). */
function emailPrefix(address: string): string {
  return (address || '').split('@')[0].split('.')[0] || 'emails';
}

/** Verify TCP reachability before handing off to node-imap. */
function tcpReachable(host: string, port: number, timeoutMs = 10_000): Promise<void> {
  return new Promise((resolve, reject) => {
    const sock = net.createConnection({ host, port, timeout: timeoutMs });
    sock.once('connect', () => { sock.destroy(); resolve(); });
    sock.once('timeout', () => { sock.destroy(); reject(new Error(`TCP timeout: ${host}:${port}`)); });
    sock.once('error', (err) => { sock.destroy(); reject(new Error(`TCP error: ${err.message}`)); });
  });
}

/** Extract address text from a mailparser address object. */
function getAddressText(addr: unknown): string {
  if (!addr) return '';
  if (Array.isArray(addr)) return addr.map((a: any) => a.text || a.address || '').join(', ');
  return (addr as any).text || (addr as any).address || '';
}

/** Recursively find files matching given extensions. */
function findFilesByExt(dir: string, extensions: string[]): string[] {
  const results: string[] = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...findFilesByExt(fullPath, extensions));
    } else if (extensions.some((ext) => entry.name.toLowerCase().endsWith(ext))) {
      results.push(fullPath);
    }
  }
  return results;
}

// ---------------------------------------------------------------------------
// EmailMonitor — one per client
// ---------------------------------------------------------------------------

class EmailMonitor extends EventEmitter {
  private client: ClientSettings;
  private imap: Imap | null = null;
  private pollTimer: NodeJS.Timeout | null = null;
  private status: EmailServiceStatus;
  private checking = false;
  private activeReject: ((err: Error) => void) | null = null;

  constructor(client: ClientSettings) {
    super();
    this.client = client;
    this.status = {
      clientId: client.id,
      connected: false,
      lastCheck: null,
      lastError: null,
      emailsProcessed: 0,
    };
  }

  // ── Lifecycle ──────────────────────────────────────────────

  async start(): Promise<void> {
    if (!this.client.enabled) {
      emailLog(`Client ${this.client.name} is disabled — skipping`);
      return;
    }

    emailLog(`Starting monitor for ${this.client.name} (${this.client.incomingEmail.address})`);
    this.emitStatus();

    try {
      await this.connect();
    } catch (err) {
      const msg = (err as Error).message;
      emailLog(`Initial connect failed for ${this.client.name}: ${msg} — will retry on poll`);
      this.status.lastError = msg;
      this.emitStatus();
    }

    this.startPolling();
  }

  async stop(): Promise<void> {
    emailLog(`Stopping monitor for ${this.client.name}`);
    this.stopPolling();
    this.disconnect();
    this.status.connected = false;
    this.emitStatus();
  }

  getStatus(): EmailServiceStatus {
    return { ...this.status };
  }

  updateClient(client: ClientSettings): void {
    this.client = client;
    // client reference updated — status uses clientId only
  }

  // ── IMAP Connection ────────────────────────────────────────

  private async connect(): Promise<void> {
    const { server, port, address, password, ssl } = this.client.incomingEmail;
    emailLog(`Connecting IMAP: ${address}@${server}:${port} tls=${ssl}`);

    // Pre-flight TCP check
    await tcpReachable(server, port);
    emailLog(`TCP reachable: ${server}:${port}`);

    return new Promise<void>((resolve, reject) => {
      let settled = false;

      // Hard 30s timeout
      const hardTimeout = setTimeout(() => {
        if (settled) return;
        settled = true;
        emailLog(`IMAP hard timeout (30s) for ${this.client.name}`);
        this.destroyImap();
        reject(new Error('IMAP connect timed out (30s)'));
      }, 30_000);
      if (typeof (hardTimeout as any).unref === 'function') (hardTimeout as any).unref();

      const settle = (action: 'resolve' | 'reject', err?: Error) => {
        if (settled) return;
        settled = true;
        clearTimeout(hardTimeout);
        action === 'resolve' ? resolve() : reject(err);
      };

      try {
        this.imap = new Imap({
          user: address,
          password,
          host: server,
          port,
          tls: ssl,
          tlsOptions: { rejectUnauthorized: false },
          authTimeout: 15_000,
          connTimeout: 15_000,
        });
      } catch (err) {
        settle('reject', err as Error);
        return;
      }

      this.imap.once('ready', () => {
        emailLog(`IMAP connected for ${this.client.name}`);
        this.status.connected = true;
        this.status.lastError = null;
        this.emitStatus();
        settle('resolve');
      });

      this.imap.once('error', (err: Error) => {
        emailLog(`IMAP error for ${this.client.name}: ${err.message}`);
        this.handleImapDisconnect(err);
        settle('reject', err);
      });

      this.imap.once('end', () => {
        emailLog(`IMAP ended for ${this.client.name}`);
        this.handleImapDisconnect(new Error('IMAP connection ended'));
      });

      this.imap.connect();
    });
  }

  private disconnect(): void {
    if (this.imap) {
      try { this.imap.end(); } catch { /* ignore */ }
      this.imap = null;
    }
  }

  private destroyImap(): void {
    if (!this.imap) return;
    try { this.imap.destroy(); } catch { /* ignore */ }
    try { this.imap.end(); } catch { /* ignore */ }
    this.imap = null;
  }

  private handleImapDisconnect(err: Error): void {
    this.status.lastError = err.message;
    this.status.connected = false;
    this.imap = null;
    this.emitStatus();
    this.emit('email:error', err, this.client.id);
    if (this.activeReject) {
      this.activeReject(err);
      this.activeReject = null;
    }
  }

  // ── Polling ────────────────────────────────────────────────

  private startPolling(): void {
    this.checkForNewEmails(); // immediate first check
    this.pollTimer = setInterval(() => this.checkForNewEmails(), 60_000);
  }

  private stopPolling(): void {
    if (this.pollTimer) {
      clearInterval(this.pollTimer);
      this.pollTimer = null;
    }
  }

  private async checkForNewEmails(): Promise<void> {
    if (this.checking) {
      emailLog(`Poll already running for ${this.client.name} — skipping`);
      return;
    }
    this.checking = true;
    try {
      // Timeout: if doPoll hangs (IMAP connection dropped silently), force-reset after 90s
      const POLL_TIMEOUT_MS = 90_000;
      await Promise.race([
        this.doPoll(),
        new Promise<void>((_, reject) =>
          setTimeout(() => reject(new Error('Poll timed out')), POLL_TIMEOUT_MS),
        ),
      ]);
    } catch (err) {
      emailLog(`Poll error for ${this.client.name}: ${(err as Error).message}`);
      // Force disconnect so next poll reconnects fresh
      this.status.connected = false;
      try { this.imap?.end(); } catch { /* ignore */ }
      this.imap = null;
    } finally {
      this.checking = false;
    }
  }

  private async doPoll(): Promise<void> {
    // Reconnect if needed
    if (!this.imap || !this.status.connected) {
      emailLog(`Not connected — reconnecting for ${this.client.name}…`);
      try {
        await this.connect();
      } catch (err) {
        this.status.lastError = (err as Error).message;
        this.emitStatus();
        return;
      }
    }

    // Handle reprocess requests before polling
    await this.checkReprocessRequests();

    emailLog(`Checking for new emails for ${this.client.name}`);
    this.status.lastCheck = new Date();

    try {
      const emails = await this.fetchUnseenEmails();
      emailLog(`Found ${emails.length} unseen emails for ${this.client.name}`);

      // Notify renderer so status bar countdown can reset
      if (emails.length > 0) {
        this.emit('email:progress', {
          clientId: this.client.id,
          message: `Found ${emails.length} unseen email(s) for ${this.client.name}`,
        });
      }

      for (const email of emails) {
        await this.processEmail(email);
      }

      this.emitStatus();
    } catch (err) {
      emailLog(`Poll failed for ${this.client.name}: ${(err as Error).message}`);
      this.status.lastError = (err as Error).message;
      this.emitStatus();
    }
  }

  // ── Per-email Processing ───────────────────────────────────

  private async processEmail(email: RuntimeEmail): Promise<void> {
    emailLog(`Email: subject="${email.subject}" from="${email.from}" uid=${email.uid} attachments=${email.attachments.length}`);

    // Skip bot-sent emails (prevent cascade loops)
    if ((email.from || '').includes('documents.websource@auto-brokerage.com')) {
      emailLog(`Skipping bot-sent email: ${email.subject}`);
      await this.safeMarkAsRead(email.uid);
      return;
    }

    // Filter for actionable attachments
    const hasPdf = email.attachments.some(
      (a) => a.contentType === 'application/pdf' || a.filename?.toLowerCase().endsWith('.pdf'),
    );
    const hasZip = email.attachments.some(
      (a) => a.filename?.toLowerCase().endsWith('.zip') ||
             a.contentType === 'application/x-zip-compressed' ||
             a.contentType === 'application/zip',
    );

    if (!hasPdf && !hasZip) {
      emailLog(`No PDF/ZIP in: ${email.subject} — skipping`);
      await this.safeMarkAsRead(email.uid);
      return;
    }

    emailLog(`Processing: ${email.subject} (${email.attachments.length} attachments)`);
    this.emit('email:received', this.toIncomingEmail(email), this.client.id);

    // Record in DB (state: saving)
    const record = recordProcessedEmail({
      clientId: this.client.id,
      messageId: email.messageId,
      subject: email.subject,
      from: email.from,
      receivedAt: email.date.toISOString(),
      processedAt: new Date().toISOString(),
      status: 'saving',
      emailSent: false,
      retryCount: 0,
    });

    // Filter saveable attachments
    const saveable = email.attachments.filter((a) => {
      const ext = (a.filename || '').toLowerCase();
      return ext.endsWith('.pdf') || ext.endsWith('.xlsx') || ext.endsWith('.zip');
    });

    try {
      const savedFiles = await this.saveAttachments(email, saveable);
      emailLog(`Saved ${savedFiles.length} files: ${savedFiles.join(', ')}`);

      const savedDir = path.dirname(savedFiles[1] || savedFiles[0]);

      updateProcessedEmail(record.id, {
        status: 'files_ready',
        outputFiles: savedFiles,
        inputDir: savedDir,
      });

      // Auto-classify & route if enabled
      if (this.client.autoProcess) {
        const pdfFiles = savedFiles.filter((f) => f.toLowerCase().endsWith('.pdf'));
        if (pdfFiles.length >= 1) {
          emailLog(`Auto-processing ${pdfFiles.length} PDFs in ${savedDir}`);
          this.emit('bl:classifyAndRoute', {
            inputDir: savedDir,
            clientId: this.client.id,
            recordId: record.id,
          });
        }
      }

      await this.safeMarkAsRead(email.uid);
    } catch (err) {
      emailLog(`Failed to process email: ${(err as Error).message}`);
      updateProcessedEmail(record.id, {
        status: 'error',
        error: (err as Error).message,
      });
      this.emit('email:error', err as Error, this.client.id);
    }
  }

  /** Convert RuntimeEmail to the IPC-safe IncomingEmail shape. */
  private toIncomingEmail(email: RuntimeEmail): IncomingEmail {
    return {
      messageId: email.messageId,
      uid: email.uid,
      subject: email.subject,
      from: email.from,
      to: email.to || '',
      date: email.date,
      body: email.body || '',
      attachments: email.attachments.map((a) => ({
        filename: a.filename,
        contentType: a.contentType,
        size: a.size,
      })),
    };
  }

  // ── IMAP Fetch ─────────────────────────────────────────────

  private fetchUnseenEmails(): Promise<RuntimeEmail[]> {
    return new Promise((resolve, reject) => {
      if (!this.imap) return reject(new Error('IMAP not connected'));
      this.activeReject = reject;

      const doOpen = () => {
        this.imap!.openBox('INBOX', false, (err, box) => {
          if (err) return reject(err);
          emailLog(`INBOX: ${box.messages.total} total, ${box.messages.unseen ?? box.messages.new} unseen`);

          const done = (emails: RuntimeEmail[]) => { this.activeReject = null; resolve(emails); };
          const fail = (e: Error) => { this.activeReject = null; reject(e); };
          this.doSearch(done, fail);
        });
      };

      // Close + STATUS to force flag refresh, then reopen
      try {
        this.imap!.closeBox(false, () => {
          try {
            this.imap!.status('INBOX', () => doOpen());
          } catch { doOpen(); }
        });
      } catch { doOpen(); }
    });
  }

  private doSearch(
    resolve: (emails: RuntimeEmail[]) => void,
    reject: (err: Error) => void,
  ): void {
    this.imap!.search(['UNSEEN'], (searchErr, uids) => {
      if (searchErr) return reject(searchErr);
      emailLog(`UNSEEN UIDs: [${uids.join(',')}]`);
      if (uids.length === 0) return resolve([]);

      // Fetch one at a time to avoid IMAP timeout on large attachments
      const emails: RuntimeEmail[] = [];
      let idx = 0;

      const fetchNext = () => {
        if (idx >= uids.length) return resolve(emails);
        const uid = uids[idx++];
        this.fetchSingleEmail(uid, emails, fetchNext, resolve);
      };

      fetchNext();
    });
  }

  private fetchSingleEmail(
    uid: number,
    emails: RuntimeEmail[],
    next: () => void,
    resolveAll: (emails: RuntimeEmail[]) => void,
  ): void {
    let fetch: Imap.ImapFetch;
    try {
      fetch = this.imap!.fetch([uid], { bodies: '', struct: true });
    } catch {
      emailLog(`Fetch create error for uid ${uid} — resolving with what we have`);
      return resolveAll(emails);
    }

    let parsed = false;
    const timer = setTimeout(() => {
      if (!parsed) { parsed = true; emailLog(`Timeout uid ${uid}, skipping`); next(); }
    }, 900_000); // 15 min per email (large attachments)

    fetch.on('message', (msg: any, seqno: number) => {
      let msgUid = 0;
      msg.once('attributes', (attrs: any) => { msgUid = attrs.uid; });

      msg.on('body', (stream: any) => {
        simpleParser(stream, (parseErr: any, mail: any) => {
          if (parsed) return;
          parsed = true;
          clearTimeout(timer);

          if (parseErr) {
            emailLog(`Parse error uid ${uid}: ${parseErr.message}`);
            return next();
          }

          const attachments: EmailAttachment[] = (mail.attachments || []).map((att: any) => ({
            filename: att.filename || 'unknown',
            contentType: att.contentType,
            content: att.content,
            size: att.size,
          }));

          emails.push({
            messageId: mail.messageId || `${Date.now()}-${seqno}`,
            uid: msgUid,
            subject: mail.subject || '(no subject)',
            from: getAddressText(mail.from),
            to: getAddressText(mail.to),
            date: mail.date || new Date(),
            body: mail.text || '',
            attachments,
          });

          next();
        });
      });
    });

    fetch.once('error', (err: any) => {
      if (!parsed) {
        parsed = true;
        clearTimeout(timer);
        emailLog(`Fetch error uid ${uid}: ${err.message}`);
        next();
      }
    });
  }

  // ── Attachment Saving ──────────────────────────────────────

  private async saveAttachments(email: RuntimeEmail, attachments: EmailAttachment[]): Promise<string[]> {
    const savedFiles: string[] = [];
    const prefix = emailPrefix(this.client.incomingEmail.address);
    const targetDir = path.join(workspacePath(), prefix);
    const safeSubject = sanitiseFilename(email.subject);
    const subFolder = path.join(targetDir, safeSubject);
    fs.mkdirSync(subFolder, { recursive: true });

    // Save email body as metadata file
    const bodyContent = [
      `From: ${email.from}`,
      `To: ${email.to}`,
      `Date: ${email.date.toISOString()}`,
      `Subject: ${email.subject}`,
      '',
      '\u2500'.repeat(50),
      '',
      email.body || '(no body)',
    ].join('\n');
    const bodyPath = path.join(subFolder, 'email.txt');
    fs.writeFileSync(bodyPath, bodyContent, 'utf-8');
    savedFiles.push(bodyPath);

    for (const att of attachments) {
      const safeName = att.filename.replace(/[<>:"/\\|?*]/g, '_');
      const filePath = path.join(subFolder, safeName);
      fs.writeFileSync(filePath, att.content);

      if (safeName.toLowerCase().endsWith('.zip')) {
        const extracted = await this.extractZipAttachment(filePath, subFolder, savedFiles);
        if (!extracted) savedFiles.push(filePath); // keep zip as fallback
      } else {
        savedFiles.push(filePath);
      }
    }

    return savedFiles;
  }

  private async extractZipAttachment(
    zipPath: string,
    destDir: string,
    savedFiles: string[],
  ): Promise<boolean> {
    try {
      emailLog(`Extracting ZIP: ${path.basename(zipPath)}`);
      await extractZip(zipPath, { dir: destDir });

      const extracted = findFilesByExt(destDir, ['.pdf', '.xlsx']);
      let count = 0;
      for (const fp of extracted) {
        // Flatten nested files to destDir root
        if (path.dirname(fp) !== destDir) {
          const dest = path.join(destDir, path.basename(fp));
          if (!fs.existsSync(dest)) {
            fs.renameSync(fp, dest);
            if (!savedFiles.includes(dest)) { savedFiles.push(dest); count++; }
          }
        } else if (!savedFiles.includes(fp)) {
          savedFiles.push(fp);
          count++;
        }
      }
      emailLog(`Extracted ${count} files from ${path.basename(zipPath)}`);
      fs.unlinkSync(zipPath); // remove zip after extraction
      return true;
    } catch (err) {
      emailLog(`ZIP extraction failed: ${(err as Error).message}`);
      return false;
    }
  }

  // ── IMAP Flag Helpers ──────────────────────────────────────

  private async safeMarkAsRead(uid: number): Promise<void> {
    if (!this.imap) return;
    return new Promise((resolve) => {
      const timer = setTimeout(resolve, 10_000);
      this.imap!.addFlags(uid, ['\\Seen'], (err) => {
        clearTimeout(timer);
        if (err) emailLog(`Failed to mark uid ${uid} as read: ${err.message}`);
        resolve();
      });
    });
  }

  private async markAsUnread(uid: number): Promise<void> {
    if (!this.imap) return;
    return new Promise((resolve) => {
      const timer = setTimeout(resolve, 10_000);
      this.imap!.delFlags(uid, ['\\Seen'], (err) => {
        clearTimeout(timer);
        if (err) emailLog(`Failed to mark uid ${uid} as unread: ${err.message}`);
        resolve();
      });
    });
  }

  // ── Reprocess Support ──────────────────────────────────────

  private async checkReprocessRequests(): Promise<void> {
    let configPath: string;
    try {
      configPath = path.join(dataPath(), 'reprocess_emails.json');
    } catch {
      return; // dataPath not initialised
    }
    if (!fs.existsSync(configPath)) return;

    try {
      const raw = fs.readFileSync(configPath, 'utf-8').trim();
      if (!raw || raw === '{}') return;

      const config: Record<string, number[]> = JSON.parse(raw);
      const uids = config[this.client.id] || [];
      if (uids.length === 0) return;

      emailLog(`Reprocess requested for ${uids.length} UIDs: ${uids.join(', ')}`);

      // Open INBOX for flag operations
      await new Promise<void>((resolve) => {
        if (!this.imap) return resolve();
        this.imap.openBox('INBOX', false, () => resolve());
      });

      for (const uid of uids) {
        await this.markAsUnread(uid);
      }

      // Close box so fetchUnseenEmails can open fresh
      await new Promise<void>((resolve) => {
        if (!this.imap) return resolve();
        this.imap.closeBox(false, () => resolve());
      });

      // Clean up config file
      delete config[this.client.id];
      if (Object.keys(config).length === 0) {
        fs.unlinkSync(configPath);
      } else {
        fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
      }

      emailLog(`Reprocess config cleared for ${this.client.name}`);
    } catch (err) {
      emailLog(`Error reading reprocess config: ${(err as Error).message}`);
    }
  }

  // ── Status Emission ────────────────────────────────────────

  private emitStatus(): void {
    this.emit('status:change', { ...this.status });
  }
}

// ---------------------------------------------------------------------------
// EmailService — singleton manager
// ---------------------------------------------------------------------------

export class EmailService extends EventEmitter {
  private monitors = new Map<string, EmailMonitor>();

  async startMonitor(client: ClientSettings): Promise<void> {
    emailLog(`startMonitor: ${client.name} (${client.id})`);

    if (this.monitors.has(client.id)) {
      emailLog(`Monitor already running for ${client.name}`);
      return;
    }

    const monitor = new EmailMonitor(client);

    // Forward all events
    const forward = (event: string) => {
      monitor.on(event, (...args: any[]) => this.emit(event, ...args));
    };
    forward('email:received');
    forward('email:error');
    forward('email:progress');
    forward('status:change');
    forward('bl:process');
    forward('bl:classifyAndRoute');

    this.monitors.set(client.id, monitor);

    try {
      await monitor.start();
    } catch (err) {
      emailLog(`Failed to start monitor for ${client.name}: ${(err as Error).message}`);
      this.monitors.delete(client.id);
      throw err;
    }
  }

  async stopMonitor(clientId: string): Promise<void> {
    const monitor = this.monitors.get(clientId);
    if (monitor) {
      await monitor.stop();
      this.monitors.delete(clientId);
    }
  }

  async stopAll(): Promise<void> {
    const stops = Array.from(this.monitors.values()).map((m) => m.stop());
    await Promise.allSettled(stops);
    this.monitors.clear();
  }

  getStatus(clientId: string): EmailServiceStatus | null {
    return this.monitors.get(clientId)?.getStatus() ?? null;
  }

  getAllStatuses(): EmailServiceStatus[] {
    return Array.from(this.monitors.values()).map((m) => m.getStatus());
  }

  updateClient(client: ClientSettings): void {
    this.monitors.get(client.id)?.updateClient(client);
  }
}

// Singleton
export const emailService = new EmailService();

// ---------------------------------------------------------------------------
// SMTP Email Sending
// ---------------------------------------------------------------------------

export async function sendEmail(client: ClientSettings, options: SendEmailOptions): Promise<void> {
  const { server, port, address, password, ssl } = client.outgoingEmail;

  const transporter = nodemailer.createTransport({
    host: server,
    port,
    secure: ssl,
    auth: { user: address, pass: password },
    tls: { rejectUnauthorized: false },
  });

  const recipients = Array.isArray(options.to) ? options.to.join(', ') : options.to;

  emailLog(`Sending email to ${recipients}: ${options.subject}`);

  const info = await transporter.sendMail({
    from: address,
    to: recipients,
    subject: options.subject,
    text: options.body,
    attachments: options.attachments?.map((a) => ({ filename: a.filename, path: a.path })),
  });

  emailLog(`Email sent: ${info.messageId}`);
}

export async function sendErrorNotification(
  client: ClientSettings,
  error: Error,
  context: string,
): Promise<void> {
  // v2.0 uses outgoingEmail; send to the same address as a self-notification
  const recipient = client.outgoingEmail.address;
  if (!recipient) {
    console.warn(TAG, `No outgoing email configured for ${client.name}`);
    return;
  }

  await sendEmail(client, {
    to: recipient,
    subject: `[AutoInvoice Error] ${client.name} - Processing Failed`,
    body: `An error occurred while processing emails for ${client.name}:\n\nContext: ${context}\nError: ${error.message}\nStack: ${error.stack ?? 'N/A'}\n\nTime: ${new Date().toISOString()}\n`,
  });
}
