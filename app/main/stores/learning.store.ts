import fs from 'fs';
import path from 'path';

interface Correction {
  id: string;
  timestamp: string;
  conversationId: string;
  originalCode: string;
  correctedCode: string;
  description: string;
  itemPattern: string;
  confidence: number;
}

interface LearnedRule {
  id: string;
  code: string;
  category: string;
  patterns: string[];
  confidence: number;
  sourceCorrections: string[];
  createdAt: string;
}

export class LearningStore {
  private correctionsPath: string;
  private learnedRulesPath: string;
  private corrections: Correction[] = [];
  private learnedRules: LearnedRule[] = [];

  constructor(baseDir: string) {
    this.correctionsPath = path.join(baseDir, 'data', 'corrections.json');
    this.learnedRulesPath = path.join(baseDir, 'data', 'learned_rules.json');
    this.load();
  }

  private load(): void {
    try {
      if (fs.existsSync(this.correctionsPath)) {
        this.corrections = JSON.parse(fs.readFileSync(this.correctionsPath, 'utf-8'));
      }
    } catch {}

    try {
      if (fs.existsSync(this.learnedRulesPath)) {
        this.learnedRules = JSON.parse(fs.readFileSync(this.learnedRulesPath, 'utf-8'));
      }
    } catch {}
  }

  private save(): void {
    fs.mkdirSync(path.dirname(this.correctionsPath), { recursive: true });
    fs.writeFileSync(this.correctionsPath, JSON.stringify(this.corrections, null, 2));
    fs.writeFileSync(this.learnedRulesPath, JSON.stringify(this.learnedRules, null, 2));
  }

  addCorrection(correction: Omit<Correction, 'id' | 'timestamp'>): Correction {
    const entry: Correction = {
      ...correction,
      id: Date.now().toString(36) + Math.random().toString(36).slice(2, 9),
      timestamp: new Date().toISOString(),
    };

    this.corrections.push(entry);
    this.save();

    // Check if we should extract a rule
    this.checkForRuleExtraction(correction.correctedCode, correction.itemPattern);

    return entry;
  }

  private checkForRuleExtraction(code: string, pattern: string): void {
    // Count similar corrections
    const similar = this.corrections.filter(
      (c) => c.correctedCode === code && c.itemPattern.toLowerCase().includes(pattern.toLowerCase().split(' ')[0])
    );

    if (similar.length >= 3) {
      // Auto-extract a learned rule
      const existing = this.learnedRules.find((r) => r.code === code);
      if (!existing) {
        const rule: LearnedRule = {
          id: `learned_${Date.now().toString(36)}`,
          code,
          category: similar[0].description || 'PRODUCTS',
          patterns: [...new Set(similar.map((s) => s.itemPattern.toUpperCase()))],
          confidence: 0.85,
          sourceCorrections: similar.map((s) => s.id),
          createdAt: new Date().toISOString(),
        };
        this.learnedRules.push(rule);
        this.save();
      }
    }
  }

  getCorrections(): Correction[] {
    return this.corrections;
  }

  getLearnedRules(): LearnedRule[] {
    return this.learnedRules;
  }
}
