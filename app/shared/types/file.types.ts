export interface FileNode {
  name: string;
  path: string;
  type: 'file' | 'directory';
  children?: FileNode[];
  extension?: string;
  size?: number;
  modifiedAt?: string;
  locked?: boolean;
  isLink?: boolean;
  isNew?: boolean;
}

export interface RecentFile {
  path: string;
  name: string;
  openedAt: string;
  type: string;
}
