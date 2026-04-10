# CARICOM Invoice Processing Pipeline
## Comprehensive Data-Driven Specification v1.0

**Document:** Complete Pipeline Specification  
**Version:** 1.0  
**Date:** 2026-02-03  
**Source:** Analysis of 50+ Processing Conversations  

---

## TABLE OF CONTENTS

1. [Executive Summary](#1-executive-summary)
2. [Success Indicators](#2-success-indicators)
3. [Extracted Directives](#3-extracted-directives)
4. [Data-Driven Architecture](#4-data-driven-architecture)
5. [Configuration Schema](#5-configuration-schema)
6. [Pipeline Stages](#6-pipeline-stages)
7. [Rule Engine](#7-rule-engine)
8. [Validation System](#8-validation-system)
9. [Learning System](#9-learning-system)
10. [Error Catalog](#10-error-catalog)

---

## 1. EXECUTIVE SUMMARY

### 1.1 Purpose

Transform invoice processing from ad-hoc LLM interactions into a **deterministic, data-driven pipeline** where:

- **Behavior is controlled by configuration files**, not hardcoded logic
- **Classification rules are data tables**, not if-else chains  
- **Validation rules are declarative schemas**, not procedural checks
- **Output is template-driven**, not code-generated
- **Learning improves data**, not code

### 1.2 Key Architectural Principle

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA-DRIVEN PIPELINE                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   ┌──────────┐     ┌──────────┐     ┌──────────┐               │
│   │ CONFIG   │────▶│ PIPELINE │────▶│ OUTPUT   │               │
│   │ (YAML)   │     │ ENGINE   │     │ (XLSX)   │               │
│   └──────────┘     └────┬─────┘     └──────────┘               │
│                         │                                       │
│        ┌────────────────┼────────────────┐                     │
│        │                │                │                     │
│        ▼                ▼                ▼                     │
│   ┌──────────┐    ┌──────────┐    ┌──────────┐                │
│   │ RULES    │    │ SCHEMA   │    │ TEMPLATE │                │
│   │ (JSON)   │    │ (JSON)   │    │ (XLSX)   │                │
│   └──────────┘    └──────────┘    └──────────┘                │
│                                                                 │
│   Code READS data → Data CONTROLS behavior                     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Procedural (OLD):** `if "shampoo" in description: code = "33051000"`  
**Data-Driven (NEW):** Rules JSON contains `{"pattern": "shampoo", "code": "33051000"}`

---

## 2. SUCCESS INDICATORS

### 2.1 Quantitative Metrics (From Successful Outputs)

| Metric | Target | Measurement |
|--------|--------|-------------|
| **Financial Variance** | $0.00 | `VARIANCE CHECK` formula result |
| **Group Variance** | $0.00 | `GROUP VERIFICATION` formula result |
| **Code Validity** | 100% | All codes are 8-digit with DUTY+UNIT+SITC |
| **Formula Errors** | 0 | No #VALUE!, #REF!, #DIV/0! |
| **Processing Time** | <20 min | For 500 items |

### 2.2 Structural Success Criteria

```yaml
structure_validation:
  every_detail_row:
    - has: TariffCode (F)
    - has: SupplierItemNum (J)  
    - has: Description (K)
    - has: Quantity (L)
    - has: Cost (O/P)
    - blank: DocumentType (A)
    - blank: InvoiceNum (C)
    - blank: Date (D)
    - format: "    {description}"  # 4-space indent
    
  every_group_row:
    - has: DocumentType = "4000-000" (A)
    - has: InvoiceNum (C)
    - has: Date (D)
    - has: TariffCode (F)
    - has: CategoryName (K)
    - format: blue_fill, bold
    
  first_group_per_invoice:
    - has: InvoiceTotal (S)
    - has: SupplierCode (Z)
    - has: SupplierName (AA)
    - has: SupplierAddress (AB)
    - has: CountryCode (AC)
    
  per_invoice:
    - has: SUBTOTAL section
    - has: VARIANCE_CHECK = $0.00
    
  multi_invoice:
    - structure: "Inv1→SUBTOTAL→Inv2→SUBTOTAL→Grand"
    - has: GRAND_TOTAL
    - has: GRAND_VARIANCE_CHECK = $0.00
```

### 2.3 Example Successful Output Structure

```
Row 1:  [Headers - 36 columns]
Row 2:  [GROUP] 4000-000 | INV001 | 2026-01-15 | 33030090 | PERFUMES (25 items) | S=$1,250 | Z=SUPP001
Row 3:      [detail] |       |            |          |     Lattafa Perfume 100ml | $50.00
Row 4:      [detail] |       |            |          |     Ard Al Zaafaran 50ml | $45.00
...
Row N:  [GROUP] 4000-000 | INV001 | 2026-01-15 | 33072000 | BODY SPRAYS (15 items)
Row N+1:    [detail] |       |            |          |     Victoria Secret Spray | $25.00
...
Row X:   SUBTOTAL (GROUPED)    | P=SUM(group P refs)
Row X+1: SUBTOTAL (DETAILS)    | P=SUM(detail P refs)  
Row X+2: GROUP VERIFICATION    | =$0.00 ✓
Row X+3: ADJUSTMENTS           | =(T2+U2+V2-W2)
Row X+4: NET TOTAL             | =(P{grouped}+P{adj})
Row X+5: VARIANCE CHECK        | =(S2-P{net})=$0.00 ✓
```

---

## 3. EXTRACTED DIRECTIVES

### 3.1 SKILLS_FIRST Directive (Root Cause Fix)

**Source:** Conversation analysis revealed repeated errors when skills not read first

```yaml
directive: SKILLS_FIRST
priority: CRITICAL
trigger: before_any_invoice_work
actions:
  - read: /mnt/project/CARICOM_CLASSIFICATION_SKILL.md
  - read: /mnt/project/TARIFF_GROUPING_SKILL.md
  - read: /mnt/project/INVOICE_CONVERSION_SKILL.md
  - read: /mnt/skills/public/xlsx/SKILL.md
  - use: CARICOM CET xlsx (NOT PDF) for verification
```

### 3.2 Column Structure Directive

**Source:** Production failures from column changes

```yaml
directive: COLUMN_STRUCTURE
priority: CRITICAL
rules:
  - NEVER drop or rename template columns
  - NEVER insert columns (only append at end)
  - Supplier Code (Z) is CRITICAL for production
  
column_mapping:
  A: DocumentType        # "4000-000" group, BLANK detail
  B: PONumber            # blank for invoices
  C: InvoiceNum          # POPULATED group, BLANK detail
  D: Date                # POPULATED group, BLANK detail
  E: Category            # group only
  F: TariffCode          # 8-digit end-node
  G: POItemNum           # tariff code (group)
  H: POItemDesc          # category name (group)
  I: SupplierItemNum     # tariff (group), SKU (detail)
  J: SupplierItemDesc    # category (group), SKU desc (detail)
  K: Quantity            # sum (group), item qty (detail)
  L: UOM                 # unit of measure
  M: Currency            # USD
  N: blank
  O: UnitCost            # avg (group), item cost (detail)
  P: TotalCost           # sum (group), item total (detail)
  Q: StatValue           # =O*K
  R: Variance            # =P-Q
  S: InvoiceTotal        # FIRST group row ONLY per invoice
  T: Freight
  U: Insurance
  V: OtherCost           # Services go here!
  W: Deductions
  ...
  Z: SupplierCode        # FIRST group row ONLY - CRITICAL
  AA: SupplierName       # FIRST group row ONLY
  AB: SupplierAddress    # FIRST group row ONLY
  AC: CountryCode        # FIRST group row ONLY
  ...
  AK: GroupBy            # tariff code (group rows only)
```

### 3.3 Classification Directive

**Source:** 50+ classification corrections across conversations

```yaml
directive: CARICOM_CLASSIFICATION
priority: HIGH
principles:
  - PRIMARY FUNCTION determines chapter (not appearance)
  - MATERIAL determines subheading (within chapter)
  - CARICOM CET xlsx is FINAL AUTHORITY
  - 8-digit END-NODES only (with DUTY+UNIT+SITC)
  - Most SPECIFIC code wins over "other" categories

validation_checks:
  - code_length: 8
  - has_duty_rate: true
  - has_unit: true  
  - has_sitc_rev4: true
  - not_category_header: true  # codes without duty are headers
```

### 3.4 Services Directive

**Source:** Services incorrectly classified as products

```yaml
directive: SERVICES_HANDLING
priority: HIGH
rule: Services are expenses in Column V (Other Cost)
reason: Services are NOT classified product line items
examples:
  - "SERVICE CHARGE $20.00" → Column V, not product row
  - "HANDLING FEE" → Column V
  - "FREIGHT" → Column T
```

### 3.5 Formula Precision Directive

**Source:** Variance errors from rounding

```yaml
directive: FORMULA_PRECISION
priority: HIGH
rules:
  - Use FULL PRECISION (no rounding)
  - Average cost: total_cost / total_qty (full precision)
  - Syntax: =P2+P26+P33 (plus operator)
  - NOT: =P2,P26,P33 (comma causes #VALUE!)
  
formula_patterns:
  Q_column: "=O{row}*K{row}"     # same row
  R_column: "=P{row}-Q{row}"     # same row
  subtotal_grouped: "=P{r1}+P{r2}+P{r3}..."  # list all group rows
  variance_check: "=(S2-P{net_row})"
  adjustments: "=(T2+U2+V2-W2)"
```

### 3.6 Row Insertion Directive

**Source:** Off-by-one errors after inserting rows

```yaml
directive: ROW_INSERTION
priority: HIGH
after_insert:
  - Fix ALL Q formulas in rows below
  - Fix ALL R formulas in rows below
  - Update SUBTOTAL to include new groups
  - Verify GROUP_VERIFICATION = $0.00
  - Verify VARIANCE_CHECK = $0.00
```

---

## 4. DATA-DRIVEN ARCHITECTURE

### 4.1 Directory Structure

```
caricom-pipeline/
├── config/
│   ├── pipeline.yaml          # Pipeline stages configuration
│   ├── columns.yaml           # Column mappings and rules
│   └── suppliers.yaml         # Known supplier mappings
├── data/
│   ├── caricom_cet.xlsx       # Official CARICOM tariff reference
│   ├── classification_db.json # Learned classifications
│   └── corrections_log.json   # Historical corrections
├── rules/
│   ├── classification_rules.json    # Classification patterns
│   ├── chapter_rules.json           # Chapter determination
│   ├── validation_rules.json        # Validation schemas
│   └── invalid_codes.json           # Known invalid codes
├── templates/
│   ├── POTemplate.xlsx        # Output template
│   └── group_format.json      # Group row formatting
├── scripts/
│   ├── pipeline_runner.py     # Main orchestrator
│   ├── rule_engine.py         # Rule processing engine
│   ├── validator.py           # Validation engine
│   └── xlsx_generator.py      # Output generator
└── prompts/
    ├── identify_product.txt   # LLM prompt templates
    └── classify_ambiguous.txt
```

### 4.2 Configuration-Driven Pipeline

**Instead of hardcoded steps, the pipeline reads its stages from YAML:**

```yaml
# config/pipeline.yaml
pipeline:
  name: "CARICOM Invoice Processing"
  version: "1.0"
  
stages:
  - name: extract
    type: script
    script: pdf_extractor.py
    input: "${input_file}"
    output: extracted_items.json
    validation: extraction_schema
    
  - name: parse
    type: script
    script: item_parser.py
    input: extracted_items.json
    output: parsed_items.json
    rules: parsing_rules.json
    
  - name: classify
    type: rule_engine
    rules: classification_rules.json
    fallback: 
      type: llm_call
      prompt: classify_ambiguous.txt
    input: parsed_items.json
    output: classified_items.json
    validation: classification_schema
    
  - name: validate_codes
    type: validator
    rules: validation_rules.json
    reference: caricom_cet.xlsx
    input: classified_items.json
    output: validated_items.json
    
  - name: group
    type: script
    script: grouping_engine.py
    config: grouping_config.yaml
    input: validated_items.json
    output: grouped_items.json
    
  - name: generate_xlsx
    type: script
    script: xlsx_generator.py
    template: POTemplate.xlsx
    columns: columns.yaml
    input: grouped_items.json
    output: "${output_file}"
    
  - name: verify
    type: validator
    checks:
      - variance_check: 0.00
      - group_verification: 0.00
      - formula_errors: 0
    input: "${output_file}"
```

---

## 5. CONFIGURATION SCHEMA

### 5.1 Column Configuration (columns.yaml)

```yaml
# config/columns.yaml
template:
  preserve_all: true    # NEVER drop columns
  freeze_names: true    # NEVER rename columns
  
columns:
  A:
    name: DocumentType
    group_value: "4000-000"
    detail_value: null  # BLANK
    
  C:
    name: InvoiceNum
    group_value: "${invoice_number}"
    detail_value: null  # BLANK
    
  D:
    name: Date
    group_value: "${invoice_date}"
    detail_value: null  # BLANK
    
  F:
    name: TariffCode
    validation:
      length: 8
      format: "\\d{8}"
      must_exist_in: caricom_cet.xlsx
      must_have: [duty_rate, unit, sitc_rev4]
      
  K:
    name: Description
    group_format: "${category_name} (${item_count} items)"
    detail_format: "    ${description}"  # 4-space indent
    
  O:
    name: UnitCost
    group_value: "${total_cost / total_qty}"  # FULL PRECISION
    precision: full
    
  P:
    name: TotalCost
    group_value: "sum"
    detail_value: "${item_total}"
    
  Q:
    name: StatValue
    formula: "=O{row}*K{row}"
    
  R:
    name: Variance
    formula: "=P{row}-Q{row}"
    expected: 0.00
    
  S:
    name: InvoiceTotal
    populate_on: first_group_per_invoice
    
  Z:
    name: SupplierCode
    populate_on: first_group_per_invoice
    critical: true
    
  AK:
    name: GroupBy
    group_value: "${tariff_code}"
    detail_value: null
```

### 5.2 Supplier Configuration (suppliers.yaml)

```yaml
# config/suppliers.yaml
suppliers:
  AMG:
    code: "AMG001"
    name: "AMG Global Distribution"
    address: "123 Distribution Way, Miami FL"
    country: "US"
    
  BEAUTYLICIOUS:
    code: "BEAUTYLICIOUS001"
    name: "Beautylicious Wholesale"
    address: "456 Beauty Blvd"
    country: "US"
    
  JINNY:
    code: "JINNY001"
    name: "Jinny Beauty Supply"
    address: "789 Supply St"
    country: "US"
    
  # Pattern matching for unknown suppliers
  patterns:
    - match: "MAR COMPANY|MAR CO|MARCO"
      map_to: MARCO
    - match: "STAR.*BEE|STARBEE"
      map_to: STARBEE
```

---

## 6. PIPELINE STAGES

### 6.1 Stage 1: PDF Extraction

**Input:** PDF file  
**Output:** Extracted text/tables  
**Configuration:** `extraction_patterns.yaml`

```yaml
# rules/extraction_patterns.yaml
patterns:
  jinny:
    invoice_number: "Invoice\\s*#?:?\\s*(\\d+)"
    date: "(\\d{1,2}/\\d{1,2}/\\d{2,4})"
    line_item: "(\\d+)\\s+(.+?)\\s+(EA|DZ|CS)\\s+(\\d+\\.\\d{2})\\s+(\\d+\\.\\d{2})$"
    
  amazon:
    invoice_number: "Order\\s+ID:\\s*(\\d{3}-\\d{7}-\\d{7})"
    line_item: "(.+?)\\s+\\$([\\d,]+\\.\\d{2})"
    
  beautylicious:
    layout: price_then_description
    line_item: "(\\d+\\.\\d{2})\\s+(.+)"
    
  default:
    line_item: "(.+?)\\s+(\\d+)\\s+(\\d+\\.\\d{2})\\s+(\\d+\\.\\d{2})$"
```

### 6.2 Stage 2: Classification

**Input:** Parsed items  
**Output:** Classified items with tariff codes  
**Configuration:** `classification_rules.json`

```json
{
  "rules": [
    {
      "id": "shampoo_001",
      "priority": 100,
      "patterns": ["SHAMPOO", "COLORSHIELD"],
      "exclude": ["CONDITIONER"],
      "code": "33051000",
      "category": "SHAMPOO",
      "confidence": 0.95
    },
    {
      "id": "body_wash_001",
      "priority": 90,
      "patterns": ["BODY WASH", "SHOWER GEL", "BODYWASH"],
      "code": "34011910",
      "category": "SOAP & BODY WASH",
      "notes": "Chapter 34 NOT Chapter 33 - cleaning preparation"
    },
    {
      "id": "ultrasonic_diffuser",
      "priority": 85,
      "patterns": ["ULTRASONIC DIFFUSER", "AROMA DIFFUSER"],
      "exclude": ["OIL BURNER", "ELECTRIC BURNER"],
      "code": "85437090",
      "category": "ULTRASONIC DIFFUSERS",
      "notes": "Uses vibration NOT heat - NOT 85167900"
    },
    {
      "id": "electric_burner",
      "priority": 85,
      "patterns": ["OIL BURNER", "ELECTRIC BURNER", "WAX WARMER"],
      "code": "85167900",
      "category": "ELECTRO-THERMIC APPLIANCES",
      "notes": "Uses heat - 85167900"
    }
  ],
  
  "chapter_determination": {
    "cleaning_products": {
      "chapter": 34,
      "not_chapter": 33,
      "reason": "Cleaning preparations with surfactants",
      "examples": ["body wash", "hand soap", "dish soap", "surface cleaner"]
    },
    "disinfectants": {
      "chapter": 38,
      "not_chapter": 33,
      "reason": "Chemical products for killing germs",
      "examples": ["lysol", "dettol", "disinfectant spray"]
    }
  },
  
  "material_overrides": {
    "display_shipper": {
      "default_material": "cardboard",
      "default_code": "48191000",
      "not_code": "73269090",
      "notes": "Display shippers from cosmetic suppliers are cardboard, not metal"
    }
  }
}
```

### 6.3 Stage 3: Code Validation

**Input:** Classified items  
**Output:** Validated items (or flagged for review)  
**Configuration:** `validation_rules.json`

```json
{
  "code_validation": {
    "required_fields_in_cet": ["DUTY RATE", "UNIT", "SITC REV 4"],
    "code_format": "^\\d{8}$",
    "reject_if": {
      "no_duty_rate": true,
      "ends_with_00_no_subdivisions": false
    }
  },
  
  "invalid_codes": {
    "96159000": {
      "reason": "Category header without duty rate",
      "replacement": "96159090",
      "or_specific": {
        "combs": "96151110",
        "hairpins": "96159010"
      }
    },
    "84145910": {
      "reason": "Code does not exist",
      "replacement": "84145110"
    },
    "85395200": {
      "reason": "Invalid subdivision",
      "replacement": "85395000"
    },
    "33049900": {
      "reason": "Category header - use end-node",
      "replacement": "33049990",
      "or_specific": {
        "sunscreen": "33049910"
      }
    }
  }
}
```

### 6.4 Stage 4: Grouping

**Input:** Validated items  
**Output:** Grouped structure  
**Configuration:** `grouping_config.yaml`

```yaml
# config/grouping_config.yaml
grouping:
  group_by: tariff_code
  order: first_occurrence
  
  group_row:
    position: top  # Group row at TOP of its items
    formatting:
      fill: "D9E1F2"  # Light blue
      font: bold
    columns:
      A: "4000-000"
      C: "${invoice_number}"
      D: "${invoice_date}"
      F: "${tariff_code}"
      G: "${tariff_code}"
      H: "${category_name} (${item_count} items)"
      I: "${tariff_code}"
      J: "${category_name} (${item_count} items)"
      K: "${sum_quantity}"
      O: "${total_cost / sum_quantity}"  # FULL PRECISION
      P: "${sum_total_cost}"
      Q: "=O{row}*K{row}"
      R: "=P{row}-Q{row}"
      AK: "${tariff_code}"
      
  detail_row:
    formatting:
      font: normal
    columns:
      A: null  # BLANK - NOT "7500-000"
      C: null  # BLANK
      D: null  # BLANK
      F: "${tariff_code}"
      I: "${supplier_item_num}"
      J: "    ${description}"  # 4-space indent
      K: "${quantity}"
      O: "${unit_cost}"
      P: "${total_cost}"
      Q: "=O{row}*K{row}"
      R: "=P{row}-Q{row}"
      AK: null  # BLANK
      
  first_group_per_invoice:
    additional_columns:
      S: "${invoice_total}"
      Z: "${supplier_code}"
      AA: "${supplier_name}"
      AB: "${supplier_address}"
      AC: "${country_code}"

  totals_section:
    structure:
      - label: "SUBTOTAL (GROUPED)"
        P: "=SUM(${all_group_P_refs})"
        Q: "=SUM(${all_group_Q_refs})"
      - label: "SUBTOTAL (DETAILS)"
        P: "=SUM(${all_detail_P_refs})"
        Q: "=SUM(${all_detail_Q_refs})"
      - label: "GROUP VERIFICATION"
        P: "=${subtotal_grouped_P}-${subtotal_details_P}"
        expected: 0.00
        format: blue_bold
      - label: "ADJUSTMENTS"
        P: "=(T2+U2+V2-W2)"
      - label: "NET TOTAL"
        P: "=(${subtotal_grouped_P}+${adjustments_P})"
      - label: "VARIANCE CHECK"
        P: "=(S2-${net_total_P})"
        expected: 0.00
        format: red_bold

  multi_invoice:
    structure: "Inv1→SUBTOTAL→Inv2→SUBTOTAL→Grand"
    per_invoice_totals: true
    grand_totals: true
```

---

## 7. RULE ENGINE

### 7.1 Rule Engine Design

The rule engine processes classification rules from JSON files:

```python
# scripts/rule_engine.py
"""
Data-Driven Rule Engine
Reads rules from JSON, applies them to items
No hardcoded classification logic
"""

import json
import re
from typing import Dict, List, Optional, Tuple

class RuleEngine:
    def __init__(self, rules_path: str):
        with open(rules_path) as f:
            self.config = json.load(f)
        self.rules = sorted(
            self.config.get('rules', []),
            key=lambda r: r.get('priority', 0),
            reverse=True
        )
        self.invalid_codes = self.config.get('invalid_codes', {})
        self.chapter_rules = self.config.get('chapter_determination', {})
        
    def classify(self, description: str, 
                 context: Optional[Dict] = None) -> Tuple[str, str, float]:
        """
        Apply rules to classify an item.
        Returns: (tariff_code, category_name, confidence)
        """
        desc_upper = description.upper()
        
        for rule in self.rules:
            if self._matches_rule(desc_upper, rule):
                code = rule['code']
                category = rule.get('category', 'PRODUCTS')
                confidence = rule.get('confidence', 0.8)
                return code, category, confidence
                
        # No rule matched - return for LLM fallback
        return None, None, 0.0
        
    def _matches_rule(self, desc: str, rule: Dict) -> bool:
        """Check if description matches rule patterns"""
        patterns = rule.get('patterns', [])
        exclude = rule.get('exclude', [])
        
        # Check exclusions first
        for excl in exclude:
            if excl in desc:
                return False
                
        # Check if any pattern matches
        for pattern in patterns:
            if pattern in desc:
                return True
                
        return False
        
    def validate_code(self, code: str, cet_lookup: Dict) -> Tuple[bool, str]:
        """
        Validate code against CET and invalid codes list.
        Returns: (is_valid, replacement_or_reason)
        """
        # Check invalid codes list
        if code in self.invalid_codes:
            replacement = self.invalid_codes[code].get('replacement')
            reason = self.invalid_codes[code].get('reason')
            return False, replacement or reason
            
        # Check CET
        if code not in cet_lookup:
            return False, f"Code {code} not found in CARICOM CET"
            
        cet_entry = cet_lookup[code]
        if not cet_entry.get('duty_rate'):
            return False, f"Code {code} is category header (no duty rate)"
            
        return True, "Valid"
        
    def add_rule(self, rule: Dict):
        """Add a new learned rule"""
        self.rules.append(rule)
        self.rules.sort(key=lambda r: r.get('priority', 0), reverse=True)
        
    def save_rules(self, path: str):
        """Persist rules to file"""
        self.config['rules'] = self.rules
        with open(path, 'w') as f:
            json.dump(self.config, f, indent=2)
```

### 7.2 Word Analysis Algorithm

**Source:** Conversation about "ultrathink" word analysis methodology

```python
# Within rule_engine.py

class WordAnalyzer:
    """
    Strip noise words, focus on high-strength product keywords
    """
    
    NOISE_WORDS = {
        # Brands (no classification value)
        'LOREAL', 'MAYBELLINE', 'GARNIER', 'REVLON', 'DOVE', 
        'NIVEA', 'PANTENE', 'TRESEMME', 'OGX', 'AUSSIE',
        
        # Sizes (no classification value)
        'ML', 'OZ', 'FL', 'GM', 'KG', 'LB', 'PC', 'PK',
        '1OZ', '2OZ', '4OZ', '8OZ', '12OZ', '16OZ',
        
        # Colors (usually no value, except hair color)
        'BLACK', 'BROWN', 'BLONDE', 'RED', 'BLUE', 'GREEN',
        'WHITE', 'PINK', 'PURPLE', 'NATURAL',
        
        # Generic words
        'NEW', 'FORMULA', 'IMPROVED', 'ORIGINAL', 'CLASSIC',
        'PACK', 'SET', 'KIT', 'BOX', 'DISPLAY'
    }
    
    HIGH_STRENGTH_KEYWORDS = {
        # Chapter 33 - Cosmetics
        'SHAMPOO': ('33051000', 'SHAMPOO'),
        'CONDITIONER': ('33051000', 'SHAMPOO & CONDITIONER'),
        'PERFUME': ('33030090', 'PERFUMES'),
        'COLOGNE': ('33030090', 'PERFUMES'),
        'LIPSTICK': ('33041000', 'LIP PRODUCTS'),
        'MASCARA': ('33042000', 'EYE MAKEUP'),
        'EYELINER': ('33042000', 'EYE MAKEUP'),
        'FOUNDATION': ('33049100', 'FACE MAKEUP'),
        'NAIL POLISH': ('33043000', 'NAIL PRODUCTS'),
        
        # Chapter 34 - Soap/Cleaning
        'BODY WASH': ('34011910', 'SOAP & BODY WASH'),
        'HAND SOAP': ('34011910', 'SOAP & BODY WASH'),
        'DISH SOAP': ('34022090', 'CLEANING PREPARATIONS'),
        
        # Chapter 67 - Hair
        'WIG': ('67041100', 'WIGS'),
        'BRAID': ('67041900', 'SYNTHETIC HAIR'),
        'WEAVE': ('67041900', 'SYNTHETIC HAIR'),
        
        # Chapter 96 - Misc
        'COMB': ('96151110', 'COMBS & BRUSHES'),
        'HAIRBRUSH': ('96039000', 'BRUSHES'),
    }
    
    def analyze(self, description: str) -> Dict:
        """
        Analyze description, strip noise, identify keywords
        """
        words = description.upper().split()
        
        # Separate noise from signal
        noise = []
        signal = []
        
        for word in words:
            clean = re.sub(r'[^A-Z]', '', word)
            if clean in self.NOISE_WORDS:
                noise.append(clean)
            elif clean:
                signal.append(clean)
                
        # Find high-strength keywords
        matches = []
        for word in signal:
            if word in self.HIGH_STRENGTH_KEYWORDS:
                code, category = self.HIGH_STRENGTH_KEYWORDS[word]
                matches.append({
                    'keyword': word,
                    'code': code,
                    'category': category,
                    'confidence': 0.9
                })
                
        return {
            'original': description,
            'noise_stripped': noise,
            'signal_words': signal,
            'keyword_matches': matches,
            'best_match': matches[0] if matches else None
        }
```

---

## 8. VALIDATION SYSTEM

### 8.1 Validation Schema

```json
{
  "extraction_schema": {
    "type": "object",
    "required": ["invoice_number", "date", "items", "total"],
    "properties": {
      "invoice_number": {"type": "string", "pattern": "^[A-Z0-9-]+$"},
      "date": {"type": "string", "format": "date"},
      "items": {
        "type": "array",
        "minItems": 1,
        "items": {
          "type": "object",
          "required": ["description", "quantity", "unit_cost", "total_cost"],
          "properties": {
            "description": {"type": "string", "minLength": 3},
            "quantity": {"type": "number", "minimum": 0},
            "unit_cost": {"type": "number", "minimum": 0},
            "total_cost": {"type": "number", "minimum": 0}
          }
        }
      },
      "total": {"type": "number", "minimum": 0}
    }
  },
  
  "classification_schema": {
    "type": "object",
    "required": ["tariff_code", "category"],
    "properties": {
      "tariff_code": {
        "type": "string",
        "pattern": "^\\d{8}$"
      },
      "category": {"type": "string"},
      "confidence": {"type": "number", "minimum": 0, "maximum": 1}
    }
  },
  
  "output_validation": {
    "variance_check": {
      "formula": "=(S2-P{net_row})",
      "expected": 0.00,
      "tolerance": 0.001
    },
    "group_verification": {
      "formula": "=P{grouped}-P{details}",
      "expected": 0.00,
      "tolerance": 0.001
    },
    "formula_errors": {
      "scan_for": ["#REF!", "#VALUE!", "#DIV/0!", "#NAME?", "#N/A"],
      "expected_count": 0
    }
  }
}
```

### 8.2 Validator Implementation

```python
# scripts/validator.py
"""
Data-Driven Validator
Reads validation rules from JSON schemas
"""

import json
import openpyxl
from typing import Dict, List, Tuple

class Validator:
    def __init__(self, rules_path: str):
        with open(rules_path) as f:
            self.rules = json.load(f)
            
    def validate_xlsx(self, filepath: str) -> Dict:
        """
        Validate Excel output against rules.
        Returns validation report.
        """
        wb = openpyxl.load_workbook(filepath, data_only=True)
        ws = wb.active
        
        report = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'checks': {}
        }
        
        # Check variance
        variance = self._find_variance_check(ws)
        report['checks']['variance_check'] = variance
        if abs(variance) > 0.001:
            report['valid'] = False
            report['errors'].append(f"VARIANCE CHECK = ${variance}, expected $0.00")
            
        # Check group verification
        group_var = self._find_group_verification(ws)
        report['checks']['group_verification'] = group_var
        if abs(group_var) > 0.001:
            report['valid'] = False
            report['errors'].append(f"GROUP VERIFICATION = ${group_var}, expected $0.00")
            
        # Check for formula errors
        formula_errors = self._scan_formula_errors(ws)
        report['checks']['formula_errors'] = len(formula_errors)
        if formula_errors:
            report['valid'] = False
            report['errors'].extend(formula_errors)
            
        # Check structure
        structure_errors = self._validate_structure(ws)
        if structure_errors:
            report['warnings'].extend(structure_errors)
            
        return report
        
    def _find_variance_check(self, ws) -> float:
        """Find VARIANCE CHECK value in worksheet"""
        for row in range(1, ws.max_row + 1):
            for col in range(1, 20):
                cell = ws.cell(row=row, column=col)
                if cell.value and 'VARIANCE CHECK' in str(cell.value).upper():
                    # Value should be in column P (16)
                    return float(ws.cell(row=row, column=16).value or 0)
        return 0.0
        
    def _find_group_verification(self, ws) -> float:
        """Find GROUP VERIFICATION value"""
        for row in range(1, ws.max_row + 1):
            for col in range(1, 20):
                cell = ws.cell(row=row, column=col)
                if cell.value and 'GROUP VERIFICATION' in str(cell.value).upper():
                    return float(ws.cell(row=row, column=16).value or 0)
        return 0.0
        
    def _scan_formula_errors(self, ws) -> List[str]:
        """Scan for Excel formula errors"""
        errors = []
        error_types = ['#REF!', '#VALUE!', '#DIV/0!', '#NAME?', '#N/A']
        
        for row in range(1, ws.max_row + 1):
            for col in range(1, ws.max_column + 1):
                cell = ws.cell(row=row, column=col)
                if cell.value and str(cell.value) in error_types:
                    col_letter = openpyxl.utils.get_column_letter(col)
                    errors.append(f"Formula error {cell.value} at {col_letter}{row}")
                    
        return errors
        
    def _validate_structure(self, ws) -> List[str]:
        """Validate row structure"""
        warnings = []
        
        for row in range(2, ws.max_row + 1):
            doc_type = ws.cell(row=row, column=1).value
            inv_num = ws.cell(row=row, column=3).value
            date = ws.cell(row=row, column=4).value
            groupby = ws.cell(row=row, column=37).value  # AK
            desc = str(ws.cell(row=row, column=11).value or '')
            
            # Skip summary rows
            if any(x in desc.upper() for x in ['SUBTOTAL', 'VARIANCE', 'NET TOTAL', 'ADJUST']):
                continue
                
            if groupby:  # Group row
                if doc_type != '4000-000':
                    warnings.append(f"Row {row}: Group row should have A='4000-000', has '{doc_type}'")
                if not inv_num:
                    warnings.append(f"Row {row}: Group row missing Invoice#")
            else:  # Detail row
                if doc_type:
                    warnings.append(f"Row {row}: Detail row should have A=BLANK, has '{doc_type}'")
                if inv_num:
                    warnings.append(f"Row {row}: Detail row should have C=BLANK, has '{inv_num}'")
                    
        return warnings
```

---

## 9. LEARNING SYSTEM

### 9.1 Correction Record Structure

```json
{
  "corrections": [
    {
      "id": "corr_001",
      "timestamp": "2026-02-03T12:00:00Z",
      "invoice_id": "307500",
      "item_description": "ULTRASONIC DIFFUSER 500ML",
      "field": "tariff_code",
      "original_value": "85167900",
      "corrected_value": "85437090",
      "reason": "Ultrasonic uses vibration, NOT heat",
      "evidence": "Web search confirmed",
      "confidence": 0.95,
      "applied_count": 0
    }
  ]
}
```

### 9.2 Rule Extraction Algorithm

```python
# scripts/learning.py
"""
Learning System - Extract rules from corrections
"""

from collections import defaultdict
from typing import Dict, List
import json

class LearningSystem:
    def __init__(self, corrections_path: str, rules_path: str):
        self.corrections_path = corrections_path
        self.rules_path = rules_path
        self.load()
        
    def load(self):
        with open(self.corrections_path) as f:
            self.corrections = json.load(f).get('corrections', [])
        with open(self.rules_path) as f:
            self.rules = json.load(f)
            
    def record_correction(self, correction: Dict):
        """Record a new correction"""
        correction['id'] = f"corr_{len(self.corrections) + 1:04d}"
        self.corrections.append(correction)
        self._save_corrections()
        self._check_for_new_rule(correction)
        
    def _check_for_new_rule(self, correction: Dict):
        """
        If 3+ similar corrections exist, extract a rule
        """
        similar = self._find_similar_corrections(correction)
        
        if len(similar) >= 3:
            rule = self._extract_rule(similar)
            if rule and not self._rule_exists(rule):
                self.rules['rules'].append(rule)
                self._save_rules()
                print(f"New rule extracted: {rule['id']}")
                
    def _find_similar_corrections(self, correction: Dict) -> List[Dict]:
        """Find corrections with same pattern"""
        similar = []
        target_code = correction['corrected_value']
        
        for c in self.corrections:
            if c['corrected_value'] == target_code:
                similar.append(c)
                
        return similar
        
    def _extract_rule(self, corrections: List[Dict]) -> Dict:
        """Extract common pattern from corrections"""
        # Find common keywords in descriptions
        descriptions = [c['item_description'].upper() for c in corrections]
        
        # Simple keyword extraction
        word_counts = defaultdict(int)
        for desc in descriptions:
            for word in desc.split():
                if len(word) > 3:  # Skip short words
                    word_counts[word] += 1
                    
        # Keywords that appear in all descriptions
        common_keywords = [
            word for word, count in word_counts.items()
            if count == len(corrections)
        ]
        
        if not common_keywords:
            return None
            
        return {
            'id': f"learned_{corrections[0]['corrected_value']}",
            'priority': 75,  # Lower than manual rules
            'patterns': common_keywords,
            'code': corrections[0]['corrected_value'],
            'category': 'LEARNED',
            'confidence': 0.7 + (len(corrections) * 0.05),
            'source': 'auto_extracted',
            'based_on_corrections': [c['id'] for c in corrections]
        }
        
    def _rule_exists(self, rule: Dict) -> bool:
        """Check if similar rule already exists"""
        for existing in self.rules.get('rules', []):
            if existing.get('code') == rule['code']:
                # Check pattern overlap
                existing_patterns = set(existing.get('patterns', []))
                new_patterns = set(rule.get('patterns', []))
                if existing_patterns & new_patterns:
                    return True
        return False
        
    def _save_corrections(self):
        with open(self.corrections_path, 'w') as f:
            json.dump({'corrections': self.corrections}, f, indent=2)
            
    def _save_rules(self):
        with open(self.rules_path, 'w') as f:
            json.dump(self.rules, f, indent=2)
```

### 9.3 ASYCUDA Feedback Loop

```python
# scripts/asycuda_feedback.py
"""
Process ASYCUDA assessed XML to learn from customs decisions
"""

import xml.etree.ElementTree as ET
from typing import Dict, List
import json

class ASYCUDAFeedback:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.load_db()
        
    def load_db(self):
        try:
            with open(self.db_path) as f:
                self.db = json.load(f)
        except FileNotFoundError:
            self.db = {'codes': {}, 'declarations': [], 'statistics': {}}
            
    def process_xml(self, xml_path: str) -> Dict:
        """
        Extract assessed tariff codes and tax data from ASYCUDA XML
        """
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        # Find namespace
        ns = {'': root.tag.split('}')[0] + '}' if '}' in root.tag else ''}
        
        declaration = {
            'id': self._find_text(root, './/Declaration_office'),
            'date': self._find_text(root, './/Date_of_registration'),
            'items': []
        }
        
        # Process each item
        for item in root.findall('.//Item'):
            code = self._find_text(item, './/Commodity_code')
            description = self._find_text(item, './/Commercial_Description')
            
            taxes = {}
            for tax_line in item.findall('.//Taxation_line'):
                tax_code = self._find_text(tax_line, 'Duty_tax_code')
                tax_rate = self._find_text(tax_line, 'Duty_tax_rate')
                taxes[tax_code] = float(tax_rate) if tax_rate else 0
                
            item_data = {
                'code': code,
                'description': description,
                'taxes': taxes,
                'preference': self._find_text(item, './/Preference_code')
            }
            declaration['items'].append(item_data)
            
            # Update code database
            self._update_code_db(code, taxes, declaration['id'])
            
        self.db['declarations'].append(declaration)
        self._save_db()
        
        return declaration
        
    def _update_code_db(self, code: str, taxes: Dict, declaration_id: str):
        """Update code database with new usage"""
        if code not in self.db['codes']:
            self.db['codes'][code] = {
                'usage_count': 0,
                'cet_rates': [],
                'declarations': []
            }
            
        self.db['codes'][code]['usage_count'] += 1
        
        cet_rate = taxes.get('CET', 0)
        if cet_rate not in self.db['codes'][code]['cet_rates']:
            self.db['codes'][code]['cet_rates'].append(cet_rate)
            
        if declaration_id not in self.db['codes'][code]['declarations']:
            self.db['codes'][code]['declarations'].append(declaration_id)
            
    def compare_with_our_classification(self, our_file: str, asycuda_xml: str) -> Dict:
        """
        Compare our classifications with customs assessed codes
        """
        assessed = self.process_xml(asycuda_xml)
        
        # Load our classifications
        # ... comparison logic ...
        
        return {
            'matches': [],
            'differences': [],
            'learning_opportunities': []
        }
        
    def _find_text(self, element, path: str) -> str:
        found = element.find(path)
        return found.text if found is not None else ''
        
    def _save_db(self):
        with open(self.db_path, 'w') as f:
            json.dump(self.db, f, indent=2)
```

---

## 10. ERROR CATALOG

### 10.1 Classification Errors

| Error | Frequency | Wrong | Right | Prevention Rule |
|-------|-----------|-------|-------|-----------------|
| Body wash in Ch. 33 | HIGH | 33051000 | 34011910 | "body wash" → Ch. 34 |
| Category codes | HIGH | 33049900 | 33049990 | Must have DUTY+UNIT+SITC |
| Ultrasonic as heat | MEDIUM | 85167900 | 85437090 | "ultrasonic" ≠ heat |
| Display as metal | MEDIUM | 73269090 | 48191000 | Cosmetic displays = cardboard |
| Invalid fan code | MEDIUM | 84145910 | 84145110 | Code doesn't exist |
| Disinfectant in Ch. 33 | MEDIUM | 33079000 | 38089400 | Kills germs → Ch. 38 |

### 10.2 Structure Errors

| Error | Cause | Prevention |
|-------|-------|------------|
| Detail with Invoice# | Copying from group | Detail C,D = BLANK |
| Missing S/Z/AA | Not setting first group | First group per invoice |
| Formula #VALUE! | Comma in SUM | Use + operator |
| Off-by-one formulas | Row insertion | Fix all Q/R below |
| Dropped columns | Template modification | NEVER drop columns |

### 10.3 Formula Errors

| Error | Example | Fix |
|-------|---------|-----|
| Wrong syntax | =P2,P26,P33 | =P2+P26+P33 |
| Missing group | SUBTOTAL misses row 158 | Update SUBTOTAL formula |
| Precision loss | Rounded avg cost | Use full precision |
| Wrong row ref | Q159=O158*L159 | Q159=O159*L159 |

---

## APPENDIX A: Quick Reference

### Tariff Code Quick Lookup

| Product Type | Code | Notes |
|--------------|------|-------|
| Shampoo | 33051000 | Hair products |
| Body Wash | 34011910 | NOT Ch. 33 |
| Perfume | 33030090 | |
| Lipstick | 33041000 | |
| Eye Makeup | 33042000 | BROW = eye |
| Face Makeup | 33049100 | |
| Skincare | 33049990 | End-node |
| Synthetic Hair | 67041900 | |
| Human Hair | 67042000 | |
| Wigs | 67041100 | |
| Combs | 96151110 | NOT 96159000 |
| Table Fans | 84145110 | NOT 84145910 |
| Ultrasonic Diffusers | 85437090 | NOT 85167900 |
| Electric Burners | 85167900 | Heat-based |
| Cardboard Displays | 48191000 | NOT 73269090 |
| Gas Stoves | 73211110 | NOT Ch. 85 |

### Formula Templates

```
Q column:     =O{row}*K{row}
R column:     =P{row}-Q{row}
SUBTOTAL:     =P{r1}+P{r2}+P{r3}+...
ADJUSTMENTS:  =(T2+U2+V2-W2)
NET TOTAL:    =(P{subtotal}+P{adj})
VARIANCE:     =(S2-P{net})
```

### Validation Checklist

```
□ Read skill files first
□ All codes 8-digit with DUTY+UNIT+SITC
□ Group rows: A=4000-000, C=Invoice#, D=Date
□ Detail rows: A=BLANK, C=BLANK, D=BLANK
□ First group per invoice: S, Z, AA, AB, AC populated
□ All formulas use + not ,
□ GROUP VERIFICATION = $0.00
□ VARIANCE CHECK = $0.00
□ No formula errors
□ Original columns preserved
□ Supplier Code (Z) populated
```

---

**END OF SPECIFICATION v1.0**

<\!-- auto-commit hook smoke test 2026-04-10T14:10:20Z -->
