import { create } from 'zustand';
import type { ClientSettings, EmailServiceStatus, ProcessedEmail } from '../../shared/types';

interface ClientState {
  clients: ClientSettings[];
  selectedClientId: string | null;
  statuses: Map<string, EmailServiceStatus>;
  processedEmails: ProcessedEmail[];
  isLoading: boolean;
  error: string | null;

  // Actions
  loadClients: () => Promise<void>;
  selectClient: (id: string | null) => void;
  createClient: (settings: Omit<ClientSettings, 'id' | 'createdAt' | 'updatedAt'>) => Promise<ClientSettings | null>;
  updateClient: (id: string, updates: Partial<ClientSettings>) => Promise<boolean>;
  deleteClient: (id: string) => Promise<boolean>;
  loadProcessedEmails: (clientId?: string) => Promise<void>;

  // Email service
  startMonitor: (clientId: string) => Promise<boolean>;
  stopMonitor: (clientId: string) => Promise<boolean>;
  updateStatus: (status: EmailServiceStatus) => void;
  sendTestEmail: (clientId: string, to: string) => Promise<boolean>;
}

export const useClientStore = create<ClientState>((set, get) => ({
  clients: [],
  selectedClientId: null,
  statuses: new Map(),
  processedEmails: [],
  isLoading: false,
  error: null,

  loadClients: async () => {
    if (!window.api) return;
    set({ isLoading: true, error: null });
    try {
      const clients = await window.api.getClients();
      const statuses = await window.api.getAllEmailStatuses();
      const statusMap = new Map<string, EmailServiceStatus>();
      statuses.forEach((s: EmailServiceStatus) => statusMap.set(s.clientId, s));
      set({ clients, statuses: statusMap, isLoading: false });
    } catch (err) {
      set({ error: (err as Error).message, isLoading: false });
    }
  },

  selectClient: (id: string | null) => {
    set({ selectedClientId: id });
    if (id) {
      get().loadProcessedEmails(id);
    }
  },

  createClient: async (settings) => {
    if (!window.api) return null;
    try {
      const client = await window.api.createClient(settings);
      set((state) => ({ clients: [...state.clients, client] }));
      return client;
    } catch (err) {
      set({ error: (err as Error).message });
      return null;
    }
  },

  updateClient: async (id, updates) => {
    if (!window.api) return false;
    try {
      const updated = await window.api.updateClient(id, updates);
      if (updated) {
        set((state) => ({
          clients: state.clients.map((c) => (c.id === id ? updated : c)),
        }));
        return true;
      }
      return false;
    } catch (err) {
      set({ error: (err as Error).message });
      return false;
    }
  },

  deleteClient: async (id) => {
    if (!window.api) return false;
    try {
      const success = await window.api.deleteClient(id);
      if (success) {
        set((state) => ({
          clients: state.clients.filter((c) => c.id !== id),
          selectedClientId: state.selectedClientId === id ? null : state.selectedClientId,
        }));
      }
      return success;
    } catch (err) {
      set({ error: (err as Error).message });
      return false;
    }
  },

  loadProcessedEmails: async (clientId?: string) => {
    if (!window.api) return;
    try {
      const emails = await window.api.getProcessedEmails(clientId, 50);
      set({ processedEmails: emails });
    } catch (err) {
      console.error('Failed to load processed emails:', err);
    }
  },

  startMonitor: async (clientId) => {
    if (!window.api) return false;
    try {
      const result = await window.api.startEmailMonitor(clientId);
      if (!result.success) {
        set({ error: result.error || 'Failed to start monitor' });
      }
      return result.success;
    } catch (err) {
      set({ error: (err as Error).message });
      return false;
    }
  },

  stopMonitor: async (clientId) => {
    if (!window.api) return false;
    try {
      const result = await window.api.stopEmailMonitor(clientId);
      return result.success;
    } catch (err) {
      set({ error: (err as Error).message });
      return false;
    }
  },

  updateStatus: (status) => {
    set((state) => {
      const newStatuses = new Map(state.statuses);
      newStatuses.set(status.clientId, status);
      return { statuses: newStatuses };
    });
  },

  sendTestEmail: async (clientId, to) => {
    if (!window.api) return false;
    try {
      const result = await window.api.sendTestEmail(clientId, to);
      if (!result.success) {
        set({ error: result.error || 'Failed to send test email' });
      }
      return result.success;
    } catch (err) {
      set({ error: (err as Error).message });
      return false;
    }
  },
}));

// Set up event listeners when store is created
if (typeof window !== 'undefined' && window.api) {
  window.api.onEmailStatus((status: EmailServiceStatus) => {
    useClientStore.getState().updateStatus(status);
  });

  // Auto-refresh processed emails list when a new email is received
  window.api.onEmailReceived(() => {
    const { selectedClientId } = useClientStore.getState();
    if (selectedClientId) {
      useClientStore.getState().loadProcessedEmails(selectedClientId);
    }
  });
}
