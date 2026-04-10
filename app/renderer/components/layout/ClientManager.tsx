import React, { useEffect, useState, useRef } from 'react';
import {
  X,
  Plus,
  Trash2,
  Play,
  Square,
  Mail,
  Folder,
  AlertCircle,
  CheckCircle,
  Clock,
  Eye,
  EyeOff,
  RefreshCw,
  Timer,
} from 'lucide-react';
import { useClientStore } from '../../stores/clientStore';
import type { ClientSettings, EmailCredentials } from '../../../shared/types';

const POLL_INTERVAL_SEC = 60;

/** Countdown timer showing seconds until next email check */
function NextCheckCountdown({ lastCheck, connected }: { lastCheck: Date | null; connected: boolean }) {
  const [secondsLeft, setSecondsLeft] = useState<number>(POLL_INTERVAL_SEC);

  useEffect(() => {
    if (!connected || !lastCheck) {
      setSecondsLeft(POLL_INTERVAL_SEC);
      return;
    }

    const update = () => {
      const elapsed = Math.floor((Date.now() - new Date(lastCheck).getTime()) / 1000);
      const remaining = Math.max(0, POLL_INTERVAL_SEC - elapsed);
      setSecondsLeft(remaining);
    };

    update();
    const interval = setInterval(update, 1000);
    return () => clearInterval(interval);
  }, [lastCheck, connected]);

  if (!connected) return null;

  const pct = ((POLL_INTERVAL_SEC - secondsLeft) / POLL_INTERVAL_SEC) * 100;

  return (
    <div className="flex items-center gap-2">
      <Timer size={12} className="text-surface-400" />
      <div className="flex items-center gap-1.5">
        <div className="w-24 h-1.5 bg-surface-700 rounded-full overflow-hidden">
          <div
            className="h-full bg-accent rounded-full transition-all duration-1000"
            style={{ width: `${pct}%` }}
          />
        </div>
        <span className="text-xs text-surface-400 tabular-nums w-8">
          {secondsLeft}s
        </span>
      </div>
    </div>
  );
}

interface ClientManagerProps {
  onClose: () => void;
}

export function ClientManager({ onClose }: ClientManagerProps) {
  const {
    clients,
    selectedClientId,
    statuses,
    processedEmails,
    isLoading,
    error,
    loadClients,
    selectClient,
    createClient,
    updateClient,
    deleteClient,
    startMonitor,
    stopMonitor,
    sendTestEmail,
  } = useClientStore();

  const [editMode, setEditMode] = useState<'view' | 'edit' | 'create'>('view');
  const [formData, setFormData] = useState<Partial<ClientSettings>>({});
  const [showPasswords, setShowPasswords] = useState({ incoming: false, outgoing: false });
  const [testEmailTo, setTestEmailTo] = useState('');

  useEffect(() => {
    loadClients();
  }, []);

  const selectedClient = clients.find((c) => c.id === selectedClientId);
  const selectedStatus = selectedClientId ? statuses.get(selectedClientId) : null;

  const handleCreateNew = () => {
    setFormData({
      name: '',
      enabled: true,
      autoProcess: true,
      markAsReadAfterProcessing: true,
      incomingEmail: { address: '', server: '', port: 993, password: '', ssl: true },
      outgoingEmail: { address: '', server: '', port: 465, password: '', ssl: true },
      watchFolder: '',
      outputRecipients: [],
      developerEmail: '',
    });
    setEditMode('create');
  };

  const handleEdit = () => {
    if (selectedClient) {
      setFormData({ ...selectedClient });
      setEditMode('edit');
    }
  };

  const handleSave = async () => {
    if (editMode === 'create') {
      const client = await createClient(formData as Omit<ClientSettings, 'id' | 'createdAt' | 'updatedAt'>);
      if (client) {
        selectClient(client.id);
        setEditMode('view');
      }
    } else if (editMode === 'edit' && selectedClientId) {
      const success = await updateClient(selectedClientId, formData);
      if (success) {
        setEditMode('view');
      }
    }
  };

  const handleDelete = async () => {
    if (selectedClientId && confirm('Are you sure you want to delete this client?')) {
      await deleteClient(selectedClientId);
    }
  };

  const handleSelectFolder = async () => {
    const folder = await window.api.selectWatchFolder();
    if (folder) {
      setFormData((prev) => ({ ...prev, watchFolder: folder }));
    }
  };

  const updateEmailCreds = (type: 'incomingEmail' | 'outgoingEmail', field: keyof EmailCredentials, value: any) => {
    setFormData((prev) => ({
      ...prev,
      [type]: { ...(prev[type] as EmailCredentials), [field]: value },
    }));
  };

  const renderClientList = () => (
    <div className="w-48 border-r border-surface-600 flex flex-col">
      <div className="p-2 border-b border-surface-600 flex items-center justify-between">
        <span className="text-xs font-medium text-surface-300">Clients</span>
        <button
          onClick={handleCreateNew}
          className="p-1 text-surface-400 hover:text-accent rounded"
          title="Add Client"
        >
          <Plus size={14} />
        </button>
      </div>
      <div className="flex-1 overflow-y-auto">
        {clients.map((client) => {
          const status = statuses.get(client.id);
          return (
            <button
              key={client.id}
              onClick={() => {
                selectClient(client.id);
                setEditMode('view');
              }}
              className={`w-full px-3 py-2 text-left text-sm flex items-center gap-2 ${
                selectedClientId === client.id
                  ? 'bg-accent/20 text-accent'
                  : 'text-surface-300 hover:bg-surface-700'
              }`}
            >
              <span
                className={`w-2 h-2 rounded-full ${
                  status?.connected ? 'bg-green-500' : client.enabled ? 'bg-yellow-500' : 'bg-surface-500'
                }`}
              />
              <span className="truncate">{client.name}</span>
            </button>
          );
        })}
      </div>
    </div>
  );

  const renderForm = () => {
    const incomingEmail = (formData.incomingEmail || {}) as EmailCredentials;
    const outgoingEmail = (formData.outgoingEmail || {}) as EmailCredentials;

    return (
      <div className="p-4 space-y-4 overflow-y-auto">
        <h3 className="text-sm font-medium text-surface-100">
          {editMode === 'create' ? 'New Client' : 'Edit Client'}
        </h3>

        {/* Basic Info */}
        <div>
          <label className="block text-xs text-surface-400 mb-1">Client Name</label>
          <input
            type="text"
            value={formData.name || ''}
            onChange={(e) => setFormData((prev) => ({ ...prev, name: e.target.value }))}
            className="w-full px-3 py-2 text-sm bg-surface-900 border border-surface-600 rounded text-surface-100"
            placeholder="e.g. WebSource"
          />
        </div>

        <div className="flex gap-4">
          <label className="flex items-center gap-2 text-xs text-surface-300">
            <input
              type="checkbox"
              checked={formData.enabled ?? true}
              onChange={(e) => setFormData((prev) => ({ ...prev, enabled: e.target.checked }))}
              className="rounded border-surface-600"
            />
            Enabled
          </label>
          <label className="flex items-center gap-2 text-xs text-surface-300">
            <input
              type="checkbox"
              checked={formData.autoProcess ?? true}
              onChange={(e) => setFormData((prev) => ({ ...prev, autoProcess: e.target.checked }))}
              className="rounded border-surface-600"
            />
            Auto-process
          </label>
          <label className="flex items-center gap-2 text-xs text-surface-300">
            <input
              type="checkbox"
              checked={formData.markAsReadAfterProcessing ?? true}
              onChange={(e) => setFormData((prev) => ({ ...prev, markAsReadAfterProcessing: e.target.checked }))}
              className="rounded border-surface-600"
            />
            Mark as read
          </label>
        </div>

        {/* Incoming Email (IMAP) */}
        <div className="border border-surface-600 rounded p-3">
          <h4 className="text-xs font-medium text-surface-300 mb-3 flex items-center gap-2">
            <Mail size={12} /> Incoming Email (IMAP)
          </h4>
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-xs text-surface-400 mb-1">Email Address</label>
              <input
                type="email"
                value={incomingEmail.address || ''}
                onChange={(e) => updateEmailCreds('incomingEmail', 'address', e.target.value)}
                className="w-full px-2 py-1.5 text-xs bg-surface-900 border border-surface-600 rounded text-surface-100"
                placeholder="documents@example.com"
              />
            </div>
            <div>
              <label className="block text-xs text-surface-400 mb-1">Server</label>
              <input
                type="text"
                value={incomingEmail.server || ''}
                onChange={(e) => updateEmailCreds('incomingEmail', 'server', e.target.value)}
                className="w-full px-2 py-1.5 text-xs bg-surface-900 border border-surface-600 rounded text-surface-100"
                placeholder="mail.example.com"
              />
            </div>
            <div>
              <label className="block text-xs text-surface-400 mb-1">Port</label>
              <input
                type="number"
                value={incomingEmail.port || 993}
                onChange={(e) => updateEmailCreds('incomingEmail', 'port', parseInt(e.target.value))}
                className="w-full px-2 py-1.5 text-xs bg-surface-900 border border-surface-600 rounded text-surface-100"
              />
            </div>
            <div className="relative">
              <label className="block text-xs text-surface-400 mb-1">Password</label>
              <input
                type={showPasswords.incoming ? 'text' : 'password'}
                value={incomingEmail.password || ''}
                onChange={(e) => updateEmailCreds('incomingEmail', 'password', e.target.value)}
                className="w-full px-2 py-1.5 pr-8 text-xs bg-surface-900 border border-surface-600 rounded text-surface-100"
              />
              <button
                type="button"
                onClick={() => setShowPasswords((p) => ({ ...p, incoming: !p.incoming }))}
                className="absolute right-2 top-6 text-surface-400"
              >
                {showPasswords.incoming ? <EyeOff size={12} /> : <Eye size={12} />}
              </button>
            </div>
          </div>
        </div>

        {/* Outgoing Email (SMTP) */}
        <div className="border border-surface-600 rounded p-3">
          <h4 className="text-xs font-medium text-surface-300 mb-3 flex items-center gap-2">
            <Mail size={12} /> Outgoing Email (SMTP)
          </h4>
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-xs text-surface-400 mb-1">Email Address</label>
              <input
                type="email"
                value={outgoingEmail.address || ''}
                onChange={(e) => updateEmailCreds('outgoingEmail', 'address', e.target.value)}
                className="w-full px-2 py-1.5 text-xs bg-surface-900 border border-surface-600 rounded text-surface-100"
                placeholder="shipments@example.com"
              />
            </div>
            <div>
              <label className="block text-xs text-surface-400 mb-1">Server</label>
              <input
                type="text"
                value={outgoingEmail.server || ''}
                onChange={(e) => updateEmailCreds('outgoingEmail', 'server', e.target.value)}
                className="w-full px-2 py-1.5 text-xs bg-surface-900 border border-surface-600 rounded text-surface-100"
                placeholder="mail.example.com"
              />
            </div>
            <div>
              <label className="block text-xs text-surface-400 mb-1">Port</label>
              <input
                type="number"
                value={outgoingEmail.port || 465}
                onChange={(e) => updateEmailCreds('outgoingEmail', 'port', parseInt(e.target.value))}
                className="w-full px-2 py-1.5 text-xs bg-surface-900 border border-surface-600 rounded text-surface-100"
              />
            </div>
            <div className="relative">
              <label className="block text-xs text-surface-400 mb-1">Password</label>
              <input
                type={showPasswords.outgoing ? 'text' : 'password'}
                value={outgoingEmail.password || ''}
                onChange={(e) => updateEmailCreds('outgoingEmail', 'password', e.target.value)}
                className="w-full px-2 py-1.5 pr-8 text-xs bg-surface-900 border border-surface-600 rounded text-surface-100"
              />
              <button
                type="button"
                onClick={() => setShowPasswords((p) => ({ ...p, outgoing: !p.outgoing }))}
                className="absolute right-2 top-6 text-surface-400"
              >
                {showPasswords.outgoing ? <EyeOff size={12} /> : <Eye size={12} />}
              </button>
            </div>
          </div>
        </div>

        {/* Watch Folder */}
        <div>
          <label className="block text-xs text-surface-400 mb-1 flex items-center gap-1">
            <Folder size={12} /> Watch Folder
          </label>
          <div className="flex gap-2">
            <input
              type="text"
              value={formData.watchFolder || ''}
              onChange={(e) => setFormData((prev) => ({ ...prev, watchFolder: e.target.value }))}
              className="flex-1 px-2 py-1.5 text-xs bg-surface-900 border border-surface-600 rounded text-surface-100"
              placeholder="D:\OneDrive\Clients\WebSource\Downloads"
            />
            <button
              onClick={handleSelectFolder}
              className="px-2 py-1.5 text-xs bg-surface-700 hover:bg-surface-600 text-surface-200 rounded"
            >
              Browse
            </button>
          </div>
        </div>

        {/* Developer Email */}
        <div>
          <label className="block text-xs text-surface-400 mb-1">Developer Email (for errors)</label>
          <input
            type="email"
            value={formData.developerEmail || ''}
            onChange={(e) => setFormData((prev) => ({ ...prev, developerEmail: e.target.value }))}
            className="w-full px-2 py-1.5 text-xs bg-surface-900 border border-surface-600 rounded text-surface-100"
            placeholder="developer@example.com"
          />
        </div>

        {/* Output Recipients */}
        <div>
          <label className="block text-xs text-surface-400 mb-1">Output Recipients (comma-separated)</label>
          <input
            type="text"
            value={(formData.outputRecipients || []).join(', ')}
            onChange={(e) =>
              setFormData((prev) => ({
                ...prev,
                outputRecipients: e.target.value.split(',').map((s) => s.trim()).filter(Boolean),
              }))
            }
            className="w-full px-2 py-1.5 text-xs bg-surface-900 border border-surface-600 rounded text-surface-100"
            placeholder="client@example.com, backup@example.com"
          />
        </div>

        {/* Save / Cancel */}
        <div className="flex gap-2 pt-2">
          <button
            onClick={handleSave}
            className="px-4 py-2 text-sm bg-accent hover:bg-accent-hover text-white rounded transition-colors"
          >
            Save
          </button>
          <button
            onClick={() => setEditMode('view')}
            className="px-4 py-2 text-sm bg-surface-700 hover:bg-surface-600 text-surface-200 rounded"
          >
            Cancel
          </button>
        </div>
      </div>
    );
  };

  const renderClientDetails = () => {
    if (!selectedClient) {
      return (
        <div className="flex-1 flex items-center justify-center text-surface-500 text-sm">
          Select a client or create a new one
        </div>
      );
    }

    return (
      <div className="flex-1 overflow-y-auto">
        <div className="p-4 space-y-4">
          {/* Header */}
          <div className="flex items-center justify-between">
            <h3 className="text-lg font-medium text-surface-100">{selectedClient.name}</h3>
            <div className="flex gap-2">
              {selectedStatus?.connected ? (
                <button
                  onClick={() => stopMonitor(selectedClient.id)}
                  className="flex items-center gap-1 px-3 py-1.5 text-xs bg-red-600 hover:bg-red-700 text-white rounded"
                >
                  <Square size={12} /> Stop
                </button>
              ) : (
                <button
                  onClick={() => startMonitor(selectedClient.id)}
                  className="flex items-center gap-1 px-3 py-1.5 text-xs bg-green-600 hover:bg-green-700 text-white rounded"
                >
                  <Play size={12} /> Start
                </button>
              )}
              <button
                onClick={handleEdit}
                className="px-3 py-1.5 text-xs bg-surface-700 hover:bg-surface-600 text-surface-200 rounded"
              >
                Edit
              </button>
              <button
                onClick={handleDelete}
                className="px-3 py-1.5 text-xs bg-red-900/50 hover:bg-red-800 text-red-200 rounded"
              >
                <Trash2 size={12} />
              </button>
            </div>
          </div>

          {/* Status */}
          <div className="flex items-center gap-4 p-3 bg-surface-900 rounded border border-surface-600">
            <div className="flex items-center gap-2">
              {selectedStatus?.connected ? (
                <CheckCircle className="text-green-500" size={16} />
              ) : (
                <AlertCircle className="text-yellow-500" size={16} />
              )}
              <span className="text-sm text-surface-200">
                {selectedStatus?.connected ? 'Connected' : 'Disconnected'}
              </span>
            </div>
            {selectedStatus?.lastCheck && (
              <div className="flex items-center gap-1 text-xs text-surface-400">
                <Clock size={12} />
                Last check: {new Date(selectedStatus.lastCheck).toLocaleTimeString()}
              </div>
            )}
            <NextCheckCountdown
              lastCheck={selectedStatus?.lastCheck ?? null}
              connected={selectedStatus?.connected ?? false}
            />
            {selectedStatus?.emailsProcessed !== undefined && (
              <div className="text-xs text-surface-400">
                Processed: {selectedStatus.emailsProcessed}
              </div>
            )}
          </div>

          {/* Details */}
          <div className="grid grid-cols-2 gap-4 text-sm">
            <div>
              <span className="text-surface-400">Incoming:</span>{' '}
              <span className="text-surface-200">{selectedClient.incomingEmail.address}</span>
            </div>
            <div>
              <span className="text-surface-400">Outgoing:</span>{' '}
              <span className="text-surface-200">{selectedClient.outgoingEmail.address}</span>
            </div>
            <div>
              <span className="text-surface-400">Watch Folder:</span>{' '}
              <span className="text-surface-200">{selectedClient.watchFolder || 'Not set'}</span>
            </div>
            <div>
              <span className="text-surface-400">Auto-process:</span>{' '}
              <span className="text-surface-200">{selectedClient.autoProcess ? 'Yes' : 'No'}</span>
            </div>
          </div>

          {/* Test Email */}
          <div className="border border-surface-600 rounded p-3">
            <h4 className="text-xs font-medium text-surface-300 mb-2">Send Test Email</h4>
            <div className="flex gap-2">
              <input
                type="email"
                value={testEmailTo}
                onChange={(e) => setTestEmailTo(e.target.value)}
                placeholder="recipient@example.com"
                className="flex-1 px-2 py-1.5 text-xs bg-surface-900 border border-surface-600 rounded text-surface-100"
              />
              <button
                onClick={() => testEmailTo && sendTestEmail(selectedClient.id, testEmailTo)}
                className="px-3 py-1.5 text-xs bg-accent hover:bg-accent-hover text-white rounded"
              >
                Send
              </button>
            </div>
          </div>

          {/* Recent Processed Emails */}
          <div>
            <h4 className="text-xs font-medium text-surface-300 mb-2 flex items-center gap-2">
              Recent Processed Emails
              <button onClick={() => useClientStore.getState().loadProcessedEmails(selectedClient.id)}>
                <RefreshCw size={12} className="text-surface-400 hover:text-surface-200" />
              </button>
            </h4>
            <div className="space-y-1">
              {processedEmails.length === 0 ? (
                <div className="text-xs text-surface-500">No emails processed yet</div>
              ) : (
                processedEmails.slice(0, 10).map((email) => (
                  <div
                    key={email.id}
                    className="flex items-center gap-2 p-2 bg-surface-900 rounded text-xs"
                  >
                    {email.status === 'completed' ? (
                      <CheckCircle className="text-green-500" size={12} />
                    ) : email.status === 'error' ? (
                      <AlertCircle className="text-red-500" size={12} />
                    ) : (
                      <Clock className="text-yellow-500" size={12} />
                    )}
                    <span className="text-surface-200 truncate flex-1">{email.subject}</span>
                    <span className="text-surface-500">
                      {new Date(email.processedAt).toLocaleString()}
                    </span>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>
      </div>
    );
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60">
      <div className="bg-surface-800 rounded-lg border border-surface-600 w-[90vw] max-w-[900px] h-[80vh] shadow-2xl flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-3 border-b border-surface-600">
          <h2 className="text-sm font-semibold text-surface-100">Email Processing Clients</h2>
          <button onClick={onClose} className="text-surface-400 hover:text-surface-100">
            <X size={16} />
          </button>
        </div>

        {/* Error Banner */}
        {error && (
          <div className="px-4 py-2 bg-red-900/50 border-b border-red-800 text-red-200 text-xs flex items-center gap-2">
            <AlertCircle size={14} />
            {error}
          </div>
        )}

        {/* Body */}
        <div className="flex-1 flex overflow-hidden">
          {renderClientList()}
          {editMode !== 'view' ? renderForm() : renderClientDetails()}
        </div>
      </div>
    </div>
  );
}
