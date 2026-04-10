import React from 'react';
import type { CellData } from '../../../shared/types';

interface Props {
  cell: CellData;
}

export function CellRenderer({ cell }: Props) {
  if (cell.type === 'empty' || cell.value === null || cell.value === undefined) {
    return null;
  }

  if (cell.type === 'error') {
    return <span className="text-red-400 font-mono">{String(cell.value)}</span>;
  }

  if (cell.type === 'number') {
    const val = cell.value as number;
    // Format as currency if numFmt suggests it
    if (cell.style?.numFmt && (cell.style.numFmt.includes('$') || cell.style.numFmt.includes('#,##0'))) {
      return <span>{formatCurrency(val)}</span>;
    }
    // Format with reasonable precision
    if (Number.isInteger(val)) {
      return <span>{val.toLocaleString()}</span>;
    }
    return <span>{val.toLocaleString(undefined, { maximumFractionDigits: 4 })}</span>;
  }

  if (cell.type === 'boolean') {
    return <span>{cell.value ? 'TRUE' : 'FALSE'}</span>;
  }

  return <span>{String(cell.value)}</span>;
}

function formatCurrency(val: number): string {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(val);
}
