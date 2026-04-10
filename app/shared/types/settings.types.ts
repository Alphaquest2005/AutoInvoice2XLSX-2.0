export interface AppSettings {
  apiKey: string;
  baseUrl: string;
  model: string;
  workspacePath: string;
  theme: 'light' | 'dark' | 'system';
  enabledStages: string[];
}
