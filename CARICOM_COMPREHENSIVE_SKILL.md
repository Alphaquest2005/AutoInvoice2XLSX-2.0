# CARICOM CUSTOMS PROCESSING: COMPREHENSIVE SKILL
## PDF → Excel Conversion, Classification & Tariff Grouping

**VERSION:** 6.0 — Unified Master Skill  
**PURPOSE:** Complete end-to-end methodology for converting PDF invoices to CARICOM customs-ready Excel files  
**AUTHORITY:** CARICOM Common External Tariff (CET) xlsx files  
**UPDATED:** February 2026

---

## TABLE OF CONTENTS

1. [Quick Start & Golden Rules](#1-quick-start--golden-rules)
2. [Phase 1: Pre-Work Setup](#2-phase-1-pre-work-setup)
3. [Phase 2: PDF Data Extraction](#3-phase-2-pdf-data-extraction)
4. [Phase 3: CARICOM Classification](#4-phase-3-caricom-classification)
5. [Phase 4: Excel Generation (POTemplate)](#5-phase-4-excel-generation-potemplate)
6. [Phase 5: Tariff Grouping](#6-phase-5-tariff-grouping)
7. [Phase 6: Multi-Invoice Processing](#7-phase-6-multi-invoice-processing)
8. [Phase 7: Quality Assurance & Validation](#8-phase-7-quality-assurance--validation)
9. [Word Analysis (Ultrathink) Methodology](#9-word-analysis-ultrathink-methodology)
10. [ASYCUDA XML Feedback Loop](#10-asycuda-xml-feedback-loop)
11. [Common Errors Catalog](#11-common-errors-catalog)
12. [Known Invalid Codes Registry](#12-known-invalid-codes-registry)
13. [Product Quick Reference Tables](#13-product-quick-reference-tables)
14. [CET xlsx Verification Guide](#14-cet-xlsx-verification-guide)
15. [Script Templates](#15-script-templates)
16. [Appendices](#16-appendices)

---

## 1. QUICK START & GOLDEN RULES

### Workflow Overview

```
PDF Invoice(s)
    ↓
[Phase 1] Read Skills + CET xlsx files
    ↓
[Phase 2] Extract ALL data from PDF (pdfplumber)
    ↓
[Phase 3] Classify items with CARICOM HS codes
    ↓
[Phase 4] Generate POTemplate Excel with formulas
    ↓
[Phase 5] Group by tariff code (optional)
    ↓
[Phase 6] Combine multi-invoice if needed
    ↓
[Phase 7] Validate: VARIANCE CHECK = $0.00
    ↓
ASYCUDA-Ready Excel Output
```

### 10 Golden Rules (NEVER VIOLATE)

| # | Rule | Consequence of Violation |
|---|------|--------------------------|
| 1 | **SKILLS_FIRST**: Always read skill files before any work | Repeating known mistakes |
| 2 | **ZERO VARIANCE**: Financial variance must equal exactly $0.00 | Invalid customs declaration |
| 3 | **END-NODES ONLY**: All codes must be 8-digit with DUTY+UNIT+SITC columns populated in CET xlsx | Customs rejection |
| 4 | **CET XLSX IS AUTHORITY**: Use xlsx files (not PDF) for code verification | Using invalid codes |
| 5 | **NEVER DROP/RENAME COLUMNS**: POTemplate columns must be preserved exactly | Production failures |
| 6 | **FUNCTION → CHAPTER**: Classify by what product DOES, not what it looks like | Wrong duty rate |
| 7 | **MATERIAL → SUBHEADING**: After chapter, material determines specific code | Wrong classification |
| 8 | **FULL PRECISION**: Never round average costs in group rows | Artificial variance |
| 9 | **SERVICES → COLUMN V**: Services/fees go in Column V, never as classified line items | Inflated dutiable value |
| 10 | **FORMULAS NOT HARDCODES**: Use Excel formulas, recalculate with LibreOffice | Stale/incorrect values |

---

## 2. PHASE 1: PRE-WORK SETUP

### 2.1 Required Files to Read

Before starting ANY invoice or tariff work, ALWAYS read:

```bash
# Project skills (MANDATORY)
/mnt/project/CARICOM_CLASSIFICATION_SKILL.md
/mnt/project/INVOICE_CONVERSION_SKILL.md
/mnt/project/TARIFF_GROUPING_SKILL.md
/mnt/project/README.md

# Excel creation best practices
/mnt/skills/public/xlsx/SKILL.md

# Reference data
/mnt/project/PMA0175797.xlsx                    # POTemplate structure reference
/mnt/project/16273revised_cet_of_caricom_hs_2017_revised_11_april_2018_for_link1300.xlsx    # CET Ch.1-30
/mnt/project/16273revised_cet_of_caricom_hs_2017_revised_11_april_2018_for_link301600.xlsx  # CET Ch.30-60
/mnt/project/16273revised_cet_of_caricom_hs_2017_revised_11_april_2018_for_link601679.xlsx  # CET Ch.60+
```

### 2.2 CET xlsx Column Structure

The CET xlsx files have varying structures per file. Key columns:

| File | HS Code Col | CET Rate Col | Description Col | UNIT Col | SITC Col |
|------|-------------|-------------|-----------------|----------|----------|
| Ch.1-30 | A (partial) + G (sub) | G or E | L or I | Varies | Varies |
| Ch.30-60 | A | E | I | Varies | Varies |
| Ch.60+ | A | Varies | H or I | Varies | Varies |

**Valid End-Node Test:** A code is valid when it has ALL THREE populated:
1. **DUTY RATE** column (shows percentage, "Free", "A", "C", or "D")
2. **UNIT** column (shows kg, l, No., m², etc.)
3. **SITC REV 4** column (shows SITC classification number)

If ANY of these are missing → it's a CATEGORY CODE (INVALID for declarations).

### 2.3 Tools & Libraries

```python
# Required
import openpyxl          # Excel read/write with formulas
import pdfplumber        # PDF text extraction
import re                # Pattern matching
from collections import OrderedDict  # Maintain grouping order

# Formula recalculation
# python /mnt/skills/public/xlsx/recalc.py output.xlsx 60
```

---

## 3. PHASE 2: PDF DATA EXTRACTION

### 3.1 Extract Strategy

```python
import pdfplumber

def extract_invoice_data(pdf_path):
    """Extract all data from invoice PDF."""
    with pdfplumber.open(pdf_path) as pdf:
        all_text = ""
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                all_text += text + "\n"
    
    # Parse header info
    header = {
        'invoice_number': extract_pattern(all_text, r'Invoice\s*#?\s*:?\s*(\S+)'),
        'date': extract_pattern(all_text, r'Date\s*:?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})'),
        'supplier_name': None,  # From letterhead/header
        'supplier_address': None,
        'grand_total': extract_total(all_text),
        'shipping': extract_shipping(all_text),
        'tax': extract_tax(all_text),
        'discount': extract_discount(all_text),
    }
    
    # Parse line items
    items = extract_line_items(all_text)
    
    return header, items
```

### 3.2 Common PDF Formats

| Supplier Type | Pattern | Description Location | Price Location |
|---------------|---------|---------------------|----------------|
| Beauty distributors | Item code + Qty + Price + Description | After code line | Same line as code |
| Amazon orders | ASIN + Product name + Price | Multiple lines per item | Below description |
| Panama suppliers | Item# + Description + Price | Same line, right-aligned | End of line |
| Chinese manufacturers | Code + Description + Qty + Unit + Total | Tabular format | Columns |

### 3.3 Multi-Line Description Handling

```python
# Combine multi-line descriptions into single string
# PDF:
#   African Formula Hair Food
#   Extra Conditioning 8oz
# → Excel Column J: "African Formula Hair Food Extra Conditioning 8oz"

# Remove warehouse/internal codes:
#   LQQKS COMB SET 6PCS Whse: WH#3
# → "LQQKS COMB SET 6PCS"
desc = re.sub(r'\s*Whse:.*$', '', desc)
```

### 3.4 Price Format Handling

| Format | Example | Extraction |
|--------|---------|------------|
| Case (CS) | 6CS @ $21.60 = $129.60 | Qty=6, Unit=$21.60, Total=$129.60 |
| Dozen (DZ) | 2DZ @ $12.00 = $24.00 | Qty=24 (2×12), Unit=$1.00, Total=$24.00 |
| Each (EA) | 10 EA @ $5.00 = $50.00 | Qty=10, Unit=$5.00, Total=$50.00 |
| Pack | 3PK @ $15.00 = $45.00 | Qty=3, Unit=$15.00, Total=$45.00 |

### 3.5 Critical Extraction Checks

```
□ Count line items in PDF vs extracted count (must match)
□ Sum of extracted line item totals vs PDF subtotal
□ Shipping/handling captured correctly
□ Tax amount captured correctly
□ Discounts captured correctly (as NEGATIVE values for credits)
□ Supplier name and address captured
□ Invoice number and date captured
□ Grand total matches PDF "Amount Due" / "Total"
```

---

## 4. PHASE 3: CARICOM CLASSIFICATION

### 4.1 Core Classification Principles

#### Principle 1: END-NODE REQUIREMENT ⚠️ CRITICAL

ALL codes MUST be 8-digit end-nodes verified against CET xlsx files:

| Check | Category (INVALID) | End-Node (VALID) |
|-------|-------------------|------------------|
| Duty Rate | Blank, dashes, or "0" with subdivisions below | Shows rate: 20%, 5%, Free, A, C, D |
| UNIT column | Empty | Populated (kg, No., l, m², etc.) |
| SITC REV 4 | Empty | Populated with SITC number |
| Subdivisions | Has codes below it | No subdivisions below |
| Pattern | Often ends in 00 | Specific digits throughout |

#### Principle 2: CLASSIFY BY PRIMARY FUNCTION

Products are classified by what they **DO**, not what they look like or who uses them.

| Function | Chapter | Examples |
|----------|---------|----------|
| Cleaning (soap/surfactants) | 34 | Body wash, dish soap, hand soap, baby wipes |
| Cosmetic/Beauty | 33 | Perfume, makeup, shampoo (HAIR ONLY), deodorant |
| Killing germs (disinfecting) | 38 | Lysol, Dettol, sanitizers |
| Hair styling | 33 | Hair gel, mousse, spray |
| Cooking (electric) | 85 | Electric stove, microwave, toaster |
| Cooking (gas) | 73 | Gas stove, gas oven |
| Ultrasonic/vibration | 85 (8543) | Ultrasonic diffusers (NOT 8516 electro-thermic) |

#### Principle 3: MATERIAL DETERMINES SUBHEADING

After identifying the chapter by function, the primary material determines the specific code:

| Material | Primary Chapter |
|----------|-----------------|
| Precious metals | Ch. 71 |
| Iron/Steel | Ch. 73 |
| Base metals | Ch. 73-76, 82-83 |
| Plastics | Ch. 39 |
| Rubber | Ch. 40 |
| Textiles | Ch. 50-63 |
| Ceramics | Ch. 69 |
| Glass | Ch. 70 |
| Wood | Ch. 44, 94 |
| Paper/Cardboard | Ch. 48 |

Example: Furniture classification by material:
- Plastic furniture → 94037000 (Ch. 94 plastic)
- Wooden furniture → 94036090 (Ch. 94 wood)
- Metal furniture → 94035000 (Ch. 94 metal)

#### Principle 4: MOST SPECIFIC CODE WINS

When multiple codes could apply:
1. Choose the code that **explicitly names** the product
2. Choose the code with the **most detailed description**
3. Avoid generic "Other" codes when specific codes exist

#### Principle 5: CET XLSX IS FINAL AUTHORITY

**Verification hierarchy:**
1. Web search (international codes) → Context/guidance only
2. Multiple customs sites → Confirmation (3+ sources same chapter)
3. **CARICOM CET xlsx files** → **FINAL AUTHORITY** (not PDF)

### 4.2 Classification Workflow

```
Step 1: IDENTIFY product → What is it? What does it do? What material?
    ↓
Step 2: DETERMINE chapter by function
    ↓
Step 3: WEB SEARCH for ambiguous items → "[product] HS code customs"
    ↓
Step 4: VERIFY in CET xlsx → Code exists + DUTY + UNIT + SITC populated
    ↓
Step 5: DOCUMENT decision → Code, duty rate, reasoning
```

### 4.3 Web Verification Protocol

**When to web search:**
- Item description is vague (just item# + brand, no product type)
- Product could belong to multiple chapters
- Unusual or unfamiliar product
- Item number only, no meaningful description

**Search templates:**
```
"[product name] HS code customs classification"
"[product] tariff code import"
"[brand] [item#] product"   ← for vague descriptions
```

**Trusted sources:** US CBP, Canada CBSA, UK HMRC, Volza, Zauba, FreightAmigo

**Requirement:** 3+ sources confirming same chapter/heading before assigning

### 4.4 Critical Chapter Distinctions

#### Chapter 33 vs Chapter 34 (MOST COMMON ERROR)

| Goes in Ch. 33 (Cosmetics) | Goes in Ch. 34 (Soap/Cleaning) |
|----------------------------|-------------------------------|
| Shampoo (HAIR ONLY) | Body wash |
| Conditioner | Hand soap (liquid/bar) |
| Perfume, cologne | Dish soap |
| Deodorant (stick/roll-on) | Surface cleaner |
| Hair gel, mousse | Baby wipes |
| Lipstick, eye makeup | Laundry detergent |
| Face powder, foundation | 2-in-1 shampoo/body wash |
| Nail polish | Cleaning wipes |

**Rule:** If it CLEANS with soap/surfactants → Chapter 34. If it BEAUTIFIES → Chapter 33.

#### Chapter 38 (Chemical Products)

| Product | Code | NOT |
|---------|------|-----|
| Lysol/Dettol | 38089400 | Not Ch. 33 or 34 |
| Hand sanitizer | 38089400 | Not Ch. 33 |
| Insecticide/Raid | 38089100 | Not Ch. 33 |
| Disinfectant spray | 38089400 | Not Ch. 34 |

#### Gas vs Electric Appliances

**DEFAULT: Unless explicitly stated "electric", assume GAS for stoves/ovens**

| Appliance | Gas (Ch. 73) | Electric (Ch. 85) |
|-----------|-------------|-------------------|
| Stove/Range | 73211110 | 85166010 |
| Oven | 73211190 | 85166090 |
| Cooker | 73211110 | 85166010 |
| Kettle | N/A | 85167900 |
| Iron | N/A | 85164000 |

#### Electro-thermic vs Ultrasonic (Chapter 85)

| Technology | Code | Products |
|------------|------|----------|
| Heat-based (electro-thermic) | 85167900 | Electric heaters, warmers |
| Ultrasonic/vibration/piezo | 85437090 | Ultrasonic diffusers, humidifiers |
| Electric motor-driven | 85094000 | Facial cleansers, food processors |

**CRITICAL:** Ultrasonic diffusers are NOT electro-thermic (85167900). They use piezoelectric vibration → 85437090.

#### Cardboard Displays vs Metal Displays

| Material | Code | NOT |
|----------|------|-----|
| Cardboard/paper displays | 48191000 | Not 73269090 (metal) |
| Metal display racks | 73269090 | Not 48191000 (paper) |

### 4.5 Product Disambiguation Rules

| Description Contains | Classify As | Code | NOT |
|---------------------|-------------|------|-----|
| "COLOR SHIELD" or "COLOR PROTECT" | Shampoo/Conditioner | 33051000 | Not hair color |
| "BROW" (eyebrow products) | Eye makeup | 33042000 | Not confused with "BROWN" |
| "BROWN" (color name) | Per product type | Various | Not eye makeup |
| "CeraVe" (any product) | Skincare | 33049990/33049910 | Always skincare regardless |
| "ColorShield Shampoo" | Shampoo | 33051000 | Not hair coloring |

---

## 5. PHASE 4: EXCEL GENERATION (POTemplate)

### 5.1 Column Layout (A-AK) — NEVER DROP OR RENAME

| Col | Header | Description | Required |
|-----|--------|-------------|----------|
| A | Document Type | 7500-000 (line items) or 4000-000 (grouped) | Yes |
| B | PO Number | Purchase Order reference | No |
| C | Supplier Invoice# | Invoice number from PDF | Yes |
| D | Date | Invoice date (YYYY-MM-DD) | Yes |
| E | Category | Item category | No |
| F | TariffCode | 8-digit CARICOM HS code | Yes |
| G | PO Item Number | PO line reference | No |
| H | PO Item Description | PO description | No |
| I | Supplier Item Number | SKU/Item code (generated) | Yes |
| J | Supplier Item Description | Item description from PDF | Yes |
| K | Quantity | Quantity from PDF | Yes |
| L | Per Unit | Per unit quantity | No |
| M | UNITS | Unit of measure (EA, CS, DZ) | No |
| N | Currency | Currency code (USD, TTD) | Yes |
| O | Cost | Unit cost | Yes |
| P | Total Cost | Extended cost (qty × unit) | Yes |
| Q | Total | Formula: =O*K | Yes |
| R | TotalCost Vs Total | Formula: =P-Q | Yes |
| S | InvoiceTotal | Invoice grand total (Row 2 only) | Yes |
| T | Total Internal Freight | Shipping/freight charges | Yes |
| U | Total Insurance | Credits/gift cards (NEGATIVE values) | Yes |
| V | Total Other Cost | Tax, fees, SERVICE CHARGES | Yes |
| W | Total Deduction | Discounts/rebates | Yes |
| X | Packages | Package count | No |
| Y | Warehouse | Warehouse code (default: 1914) | No |
| Z | Supplier Code | Supplier identifier | No |
| AA | Supplier Name | Supplier company name | Yes |
| AB | Supplier Address | Supplier full address | Yes |
| AC | Country Code | 2-letter country code | Yes |
| AD | Instructions | Special instructions | No |
| AE | Previous Declaration | Prior declaration reference | No |
| AF | Financial Information | Financial notes | No |
| AG | Gallons | Volume (gallons) | No |
| AH | Liters | Volume (liters) | No |
| AI | INVTotalCost | Invoice cost comparison | No |
| AJ | POTotalCost | PO cost comparison | No |
| AK | GroupBy | Tariff code (for grouping) | No |

### 5.2 Row Population Rules

#### Row 1: Headers (exactly as listed above)

#### Row 2: First Item + ALL Reference Data
```
A2:  7500-000
C2:  Invoice Number
D2:  Invoice Date
F2:  First item tariff code
I2:  First item supplier item number
J2:  First item description
K2:  First item quantity
N2:  Currency (e.g., USD)
O2:  First item unit cost
P2:  First item total cost
Q2:  =O2*K2
R2:  =P2-Q2

S2:  INVOICE GRAND TOTAL ← CRITICAL
T2:  Shipping/Freight
U2:  Credits (NEGATIVE, e.g., -25.00)
V2:  Tax and fees + SERVICE CHARGES
W2:  Discounts

Y2:  1914 (warehouse code)
AA2: Supplier Name
AB2: Supplier Address
AC2: Country Code (US, TT, CN, PA, etc.)
```

#### Rows 3+: Additional Line Items
```
A:  (blank for line items in ungrouped; 7500-000 if populated)
C:  Invoice Number (repeat)
D:  Date (repeat)
F:  Tariff code
I:  Supplier item number
J:  Item description
K:  Quantity
N:  Currency
O:  Unit cost
P:  Total cost
Q:  =O{row}*K{row}
R:  =P{row}-Q{row}
```

### 5.3 Cost Classification Rules

| Column | Name | Include | Sign |
|--------|------|---------|------|
| T | Total Internal Freight | Shipping, handling, delivery, freight surcharges | Positive |
| U | Total Insurance | Gift cards, store credit, promotional credits, reward points | **NEGATIVE** |
| V | Total Other Cost | Sales tax, document fees, customs duty, **SERVICE CHARGES** | Positive |
| W | Total Deduction | Supplier discounts, volume rebates, early payment discounts | Positive |

**CRITICAL:** Services (handling fees, processing charges) → Column V. NEVER classify services as product line items — this artificially inflates dutiable goods value.

### 5.4 Adjustment Formula

```excel
ADJUSTMENTS = (T2 + U2 + V2 - W2)
```

Note: U2 should already be negative, so adding it effectively subtracts. W2 is positive, so we subtract it.

### 5.5 Summary Section (After Last Data Row)

Assuming last data row is N:

| Row | Column J Label | Column P Formula | Column Q Formula |
|-----|---------------|------------------|------------------|
| N+1 | SUBTOTAL | =SUM(P2:P{N}) | =SUM(Q2:Q{N}) |
| N+2 | ADJUSTMENTS | =(T2+U2+V2-W2) | =(T2+U2+V2-W2) |
| N+3 | NET TOTAL | =(P{N+1}+P{N+2}) | =(Q{N+1}+Q{N+2}) |
| N+4 | *(blank)* | | |
| N+5 | VARIANCE CHECK | =(S2-P{N+3}) | =(S2-Q{N+3}) |

**VARIANCE CHECK must equal exactly $0.00**

### 5.6 Supplier Item Number Generation

Every line item MUST have a supplier item number (Column I).

**Format:** `BRAND-CATEGORY-SIZE-VARIANT`

| Product | Generated SKU |
|---------|---------------|
| Realistic 3x Ghana Braid 50" 1B | R-3GB-50-1B |
| L.A. Girl Lipstick Red 5ml | LAG-LIP-RD-5 |
| African Formula Hair Food 8oz | AFHF-8OZ |
| Dove Body Wash 16oz | DOVE-BWS-16 |

If invoice provides item/SKU codes, use those instead.

### 5.7 Country Codes

| Code | Country | Code | Country |
|------|---------|------|---------|
| US | United States | PA | Panama |
| TT | Trinidad and Tobago | CN | China |
| JM | Jamaica | UK | United Kingdom |
| BB | Barbados | CA | Canada |
| GD | Grenada | PR | Puerto Rico |

---

## 6. PHASE 5: TARIFF GROUPING

### 6.1 Overview

Groups invoice line items by CARICOM tariff code. **Group summary rows go at the TOP** of each classification, followed by detail rows.

### 6.2 Output Structure

```
Row 1:  [Headers]
Row 2:  [GROUP] 33030090 | PERFUMES (25 items)     | $1,250.00 | InvoiceTotal + Supplier Info
Row 3:      [detail] |     Lattafa Perfume 100ml     | $50.00
Row 4:      [detail] |     Ard Al Zaafaran 50ml      | $45.00
...
Row N:  [GROUP] 33072000 | BODY SPRAYS (15 items)  | $375.00
Row N+1:    [detail] |     Victoria Secret Spray     | $25.00
...
SUBTOTAL (GROUPED)     | $4,500.00
SUBTOTAL (DETAILS)     | $4,500.00
GROUP VERIFICATION     | $0.00 ✓
ADJUSTMENTS            | $500.00
NET TOTAL              | $5,000.00

VARIANCE CHECK         | $0.00 ✓
```

### 6.3 Group Row Structure

| Column | Value | Notes |
|--------|-------|-------|
| A | 4000-000 | Document Type for grouped |
| C | Invoice Number | Populated on ALL group rows |
| D | Invoice Date | Populated on ALL group rows |
| F | Tariff Code | 8-digit code |
| G | Tariff Code | Acts as PO Item # |
| H | Category Name (X items) | Descriptive category |
| I | Tariff Code | Acts as Supplier Item # |
| J | Category Name (X items) | Same as H |
| K | Sum of quantities | Total items in group |
| O | Average cost | **FULL PRECISION** (total_cost / total_qty) |
| P | Sum of costs | Total value of group |
| Q | =O*K | Calculated total |
| R | =P-Q | Variance (must be $0.00) |
| AK | Tariff Code | GroupBy identifier |

**FIRST group row (Row 2) ALSO has:**
- S2: InvoiceTotal
- T2-W2: Adjustments
- Z2: Supplier Code
- AA2: Supplier Name
- AB2: Supplier Address
- AC2: Country Code

**Formatting:** Blue fill (#D9E1F2), Bold font

### 6.4 Detail Row Structure

| Column | Value | Notes |
|--------|-------|-------|
| A | *(blank)* | No document type |
| C | *(BLANK)* | CLEARED — distinguishes from group rows |
| D | *(BLANK)* | CLEARED — distinguishes from group rows |
| F | Tariff Code | Same as group |
| I | Original Supplier Item # | From original data |
| J | "    " + Description | **4-space indent prefix** |
| K | Item quantity | Individual item qty |
| O | Item unit cost | Individual unit cost |
| P | Item total cost | Individual total |
| Q | =O*K | Calculated total |
| R | =P-Q | Variance |
| AK | *(blank)* | Empty — not a group row |

**Formatting:** Normal font (size 10)

### 6.5 Average Cost: FULL PRECISION ⚠️

```python
# CORRECT: Full precision
avg_cost = total_cost / total_qty  # e.g., 3.7142857142857144

# WRONG: Rounded (causes variance)
avg_cost = round(total_cost / total_qty, 2)  # e.g., 3.71 — creates $0.01+ variance
```

### 6.6 Category Name Generation

```python
CATEGORY_MAP = {
    'PERFUMES': ['PERFUME', 'EDP', 'EAU DE PARFUM', 'COLOGNE'],
    'BODY SPRAYS': ['BODY SPRAY', 'BODY MIST', 'DEODORANT SPRAY'],
    'SHAMPOO & CONDITIONER': ['SHAMPOO', 'CONDITIONER'],
    'BODY WASH & SOAP': ['BODY WASH', 'SOAP', 'CLEANSER'],
    'HAIR CARE': ['HAIR OIL', 'HAIR CREAM', 'HAIR FOOD'],
    'HAIR COLOR': ['HAIR COLOR', 'DYE', 'TINT'],
    'SYNTHETIC HAIR': ['BRAID', 'SYNTHETIC', 'WEAVE'],
    'HUMAN HAIR': ['HUMAN HAIR', 'REMI', 'REMY'],
    'COSMETICS': ['LIPSTICK', 'MASCARA', 'MAKEUP', 'EYELINER'],
    'NAIL PRODUCTS': ['NAIL', 'POLISH', 'MANICURE'],
    'LOTION & CREAM': ['LOTION', 'CREAM', 'MOISTUR'],
    'CLEANING PRODUCTS': ['CLEANER', 'DETERGENT', 'DISH'],
    'AIR FRESHENERS': ['AIR FRESH', 'FRESHENER'],
    'BABY PRODUCTS': ['BABY', 'DIAPER', 'INFANT'],
    'DRINKS': ['DRINK', 'JUICE', 'BEVERAGE', 'COFFEE'],
}
# Format: "PERFUMES (25 items)"
```

### 6.7 Totals Section Formulas

| Label | Column P Formula | Column Q Formula |
|-------|------------------|------------------|
| SUBTOTAL (GROUPED) | =SUM of all group row P values | =SUM of all group row Q values |
| SUBTOTAL (DETAILS) | =SUM of all detail row P values | =SUM of all detail row Q values |
| GROUP VERIFICATION | =P{grouped} - P{details} | =Q{grouped} - Q{details} |
| ADJUSTMENTS | =(T2+U2+V2-W2) | =(T2+U2+V2-W2) |
| NET TOTAL | =(P{grouped}+P{adj}) | =(Q{grouped}+Q{adj}) |
| VARIANCE CHECK | =(S2-P{net}) | =(S2-Q{net}) |

**Use `+` operator in SUM formulas (not commas):**
```excel
# CORRECT:
=P2+P26+P33+P41

# WRONG (causes #VALUE!):
=P2,P26,P33,P41
```

### 6.8 Formula Repair After Row Insertion

When inserting rows into a grouped sheet, formulas in rows BELOW the insertion point may shift incorrectly. Always verify:

```python
# After inserting rows, check and fix Q/R formulas
for row in range(insert_point, last_row + 1):
    ws.cell(row=row, column=17).value = f'=O{row}*K{row}'   # Q
    ws.cell(row=row, column=18).value = f'=P{row}-Q{row}'    # R

# Also update VERIFY formulas to reference ALL group rows
```

### 6.9 Formatting

```python
from openpyxl.styles import Font, PatternFill

# Group rows
group_fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
group_font = Font(bold=True, size=11)

# Detail rows
detail_font = Font(size=10)
# Description: "    " + original (4-space indent)

# GROUP VERIFICATION label
verify_font = Font(bold=True, color="0000FF")

# VARIANCE CHECK label
variance_font = Font(bold=True, color="FF0000")
```

---

## 7. PHASE 6: MULTI-INVOICE PROCESSING

### 7.1 Structure: Separated Invoice Sections

When processing multiple invoices into ONE xlsx file:

```
┌─────────────────────────────────────────────────────┐
│ INVOICE #1 DATA                                     │
│   Group rows + Detail rows (all Invoice 1 items)    │
├─────────────────────────────────────────────────────┤
│ ═══ INVOICE #1 TOTALS ═══                           │
│   SUBTOTAL (GROUPED)    $13,817.60                  │
│   ADJUSTMENTS           $0.00                       │
│   NET TOTAL             $13,817.60                  │
│   VARIANCE CHECK        $0.00 ✓                     │
├─────────────────────────────────────────────────────┤
│ INVOICE #2 DATA                                     │
│   Group rows + Detail rows (all Invoice 2 items)    │
├─────────────────────────────────────────────────────┤
│ ═══ INVOICE #2 TOTALS ═══                           │
│   SUBTOTAL (GROUPED)    $11,136.00                  │
│   ADJUSTMENTS           $0.00                       │
│   NET TOTAL             $11,136.00                  │
│   VARIANCE CHECK        $0.00 ✓                     │
├─────────────────────────────────────────────────────┤
│ ═══════════════════════════════════════════════════ │
│ GRAND TOTALS                                        │
│   GRAND SUBTOTAL        $24,953.60                  │
│   GRAND NET TOTAL       $24,953.60                  │
│   GRAND VARIANCE CHECK  $0.00 ✓                     │
└─────────────────────────────────────────────────────┘
```

### 7.2 Rules for Multi-Invoice

1. **NEVER interleave invoices** — keep each invoice's data together
2. **Each invoice has its own VARIANCE CHECK** = $0.00
3. **First group row per invoice** has S column (InvoiceTotal) + Z/AA/AB/AC (supplier info)
4. **Grand section** at the bottom has GRAND VARIANCE CHECK = $0.00
5. **Group rows for each invoice** have that invoice's Invoice# in column C and Date in column D

### 7.3 Per-Invoice Verification

Each invoice section ends with:

```
SUBTOTAL (GROUPED)     =SUM of this invoice's group P values
ADJUSTMENTS            =(T{first_row}+U{first_row}+V{first_row}-W{first_row})
NET TOTAL              =SUBTOTAL + ADJUSTMENTS
INVOICE TOTAL          =S{first_row}  (reference for clarity)
VARIANCE CHECK         =INVOICE TOTAL - NET TOTAL = $0.00
```

### 7.4 Grand Total Verification

```
SUBTOTAL (ALL GROUPED)      =SUM of all invoice subtotals
TOTAL ADJUSTMENTS           =SUM of all adjustment rows
GRAND NET TOTAL             =SUBTOTAL + ADJUSTMENTS
COMBINED INVOICE TOTALS     =SUM of all S values
GRAND VARIANCE CHECK        =COMBINED - GRAND NET = $0.00
```

---

## 8. PHASE 7: QUALITY ASSURANCE & VALIDATION

### 8.1 Pre-Delivery Checklist

```
DATA COMPLETENESS:
□ All line items extracted from PDF(s)
□ All items have tariff codes (Column F)
□ All items have supplier item numbers (Column I)
□ All items have descriptions (Column J)
□ All items have quantities and costs (K, O, P)
□ Invoice total in S2 (or S of first group row per invoice)
□ Supplier info in AA, AB, AC
□ Country code is correct 2-letter code

CLASSIFICATION:
□ All codes are 8-digit end-nodes
□ All codes verified in CET xlsx (DUTY + UNIT + SITC populated)
□ Cleaning products in Ch. 34 (not 33)
□ Disinfectants in Ch. 38 (not 33/34)
□ Gas appliances in Ch. 73 (unless explicitly electric)
□ No known invalid codes used (see Section 12)
□ Services in Column V (not as line items)

FORMULAS:
□ All rows have Q=O*K and R=P-Q formulas
□ Summary section complete
□ VARIANCE CHECK = $0.00 in both P and Q columns
□ GROUP VERIFICATION = $0.00 (if grouped)
□ All formulas recalculated with LibreOffice

FORMATTING (if grouped):
□ Group rows have blue fill (#D9E1F2) and bold
□ Detail rows have 4-space indented descriptions
□ Detail rows have C,D (Invoice#, Date) CLEARED
□ Group rows have C,D populated
□ Column AK populated on group rows only
```

### 8.2 Recalculation Command

```bash
python /mnt/skills/public/xlsx/recalc.py output.xlsx 60
```

Always verify the recalculation output shows `"status": "success"` and `"total_errors": 0`.

### 8.3 Variance Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| Variance ≠ 0 | Missing items | Compare PDF line count vs extracted count |
| Variance ≠ 0 | Wrong unit price | Check case/dozen conversions |
| Variance ≠ 0 | Missing adjustments | Check shipping/tax/discount captured |
| Variance ≠ 0 | Credits as positive | Gift cards/credits in U must be NEGATIVE |
| Variance ≠ 0 | Services as items | Move to Column V |
| GROUP VERIFY ≠ 0 | Rounded average cost | Use full precision in group O column |
| GROUP VERIFY ≠ 0 | Missing detail rows | Verify all items included |
| #VALUE! errors | Comma in SUM | Use + operator between cell refs |

---

## 9. WORD ANALYSIS (ULTRATHINK) METHODOLOGY

### 9.1 Purpose

For large batches (hundreds/thousands of items), systematically process descriptions:

```
Phase 1: STRIP noise words (brands, sizes, colors) → No classification value
Phase 2: CLASSIFY high-strength keywords first → ~80% of items
Phase 3: RESOLVE context-dependent keywords → ~15% of items
Phase 4: REVIEW remaining manually/web search → ~5% of items
```

### 9.2 Phase 1: Noise Words (Strip Out)

#### Size/Quantity Patterns
```regex
\d+\.?\d*\s*OZ       # 8OZ, 16.5OZ
\d+\.?\d*\s*ML       # 300ml, 500ML
\d+\.?\d*\s*LBS?     # 2 LBS
\d+\s*PC/?S?         # 12PC, 24PCS
\d+/CS               # 12/CS, 24/CS
\d+/DS               # 6/DS
\d+EA                # 12EA
\d+DZ                # 1DZ
\d+CM                # 10CM
\d+"                 # 3/4", 1/2"
\d+\s*GAL            # 4 GALLON
```

#### Common Brand Names (Beauty Supply)
```
LQQKS, BEAUT, LEBELAGE, GALAXY, ZAKAT, TENZERO, CLUBMAN,
COCOCARE, HANBI, ORS, PALMER, JFM, ASHLEY LEE, S-CURL,
AMPRO, STELLA, TUBBEES, GOT2B, CAFLON, AAA, TGIN, CANTU,
NAIROBI, PROFECTIV, ECO-STYLE, ORGANIX, APHOGEE, TAHA,
AUNT JACKIE'S, MALIBU, EDEN, SASSI, CURLY, BTL,
VO5, SNF, GARNIER, REVLON, DOVE, NIVEA, PANTENE, TRESEMME, OGX
```

#### Color Words
```
BLACK, WHITE, GOLD, PINK, BLUE, RED, GREEN, BROWN, SILVER,
PURPLE, CLEAR, COPPER, LAVENDER, AQUA, NATURAL, BLONDE,
GRAY, GREY, BEIGE, PASTEL, ASSORTED, MIX
```

#### Packaging/Generic Words
```
DISPLAY, BOX, JAR, TUBE, BOTTLE, PACK, PKT, SET, KIT,
CASE, CARTON, BAG, POUCH, BLISTER, CARD, THE, AND, FOR,
WITH, IN, OF, NEW, BONUS, REGULAR, LARGE, SMALL, JUMBO
```

### 9.3 Phase 2: High-Strength Classification Keywords

| Keyword | → Tariff Code | Category |
|---------|---------------|----------|
| SHAMPOO | 33051000 | Shampoo |
| CONDITIONER | 33051000 | Shampoo & Conditioner |
| PERFUME, EDP, COLOGNE | 33030090 | Perfumes |
| LIPSTICK | 33041000 | Lip Products |
| MASCARA, EYELINER | 33042000 | Eye Makeup |
| EYEBROW, BROW | 33042000 | Eye Makeup |
| FOUNDATION, POWDER | 33049100 | Face Makeup |
| NAIL POLISH | 33043000 | Nail Products |
| DEODORANT | 33072000 | Deodorants |
| BODY WASH, SHOWER GEL | 34011190 | Body Wash & Soap |
| HAND SOAP, BAR SOAP | 34011190 | Soap |
| DISH SOAP | 34022090 | Cleaning |
| BABY WIPES | 34011190 | Soap Products |
| BRAID, WEAVE | 67041900 | Synthetic Hair |
| WIG | 67041100 | Wigs |
| HUMAN HAIR, REMI | 67042000 | Human Hair |
| COMB | 96151110 | Combs |
| HAIRPIN, BOBBY PIN | 96159010 | Hair Pins |
| HAIR CLIP | 96159090 | Hair Accessories |
| HAIR BRUSH | 96032990 | Brushes |

### 9.4 Phase 3: Context-Dependent Keywords

| Keyword | + Context | → Result |
|---------|-----------|----------|
| BRUSH | + HAIR/STYLING | 96032990 (Brush) |
| BRUSH | + TOOTH/DENTAL | 96032100 (Toothbrush) |
| BRUSH | + NAIL | 96034090 (Paint brush) |
| OIL | + HAIR/SCALP | 33059000 (Hair prep) |
| OIL | + BODY/SKIN | 33049910 (Body lotion) |
| OIL | + ESSENTIAL/AROMA | 33012990 (Essential oils) |
| CREAM | + HAIR/STYLING | 33059000 (Hair prep) |
| CREAM | + FACE/SKIN | 33049910 (Skincare) |
| CREAM | + BLEACH | 33059000 (Hair prep) |
| LOTION | + BODY | 33049910 (Body lotion) |
| LOTION | + SETTING/STYLING | 33059000 (Hair styling) |
| CLIP | + HAIR | 96159090 (Hair accessory) |
| CLIP | + NAIL | 82142000 (Manicure tool) |

### 9.5 Phase 4: False Positive Patterns

| Pattern | Appears to be | Actually is |
|---------|--------------|-------------|
| "COMB THRU" | Comb (96151110) | Styling product (33059000) |
| "HAIR CLIP" | Lip product (CLIP ≠ LIP) | Hair accessory (96159090) |
| "BROWN LIPSTICK" | Eye makeup (BROWN ≠ BROW) | Lip product (33041000) |
| "COLOR SHIELD" | Hair color | Shampoo (33051000) |
| "EDGE CONTROL" | Blade/razor | Hair styling (33059000) |

---

## 10. ASYCUDA XML FEEDBACK LOOP

### 10.1 Purpose

Use assessed ASYCUDA XML files to validate and improve classifications:

```
Process invoice → Excel → Generate ASYCUDA XML → Import to ASYCUDA
    ↓
Customs assesses declaration
    ↓
Export assessed XML from ASYCUDA
    ↓
Upload assessed XML → Extract codes + taxes
    ↓
Compare: our classifications vs assessed
    ↓
Update classification database → Improve future accuracy
```

### 10.2 XML Data Extraction

Key XPaths:
```xml
/ASYCUDA/Item/Tarification/HScode/Commodity_code     → Actual HS code used
/ASYCUDA/Item/Tarification/Preference_code            → CCM (CARICOM pref)
/ASYCUDA/Item/Goods_description/Description_of_goods  → CET description
/ASYCUDA/Item/Goods_description/Commercial_Description → Our description
/ASYCUDA/Item/Taxation/Taxation_line/Duty_tax_code    → CET, CSC, VAT, EXT
/ASYCUDA/Item/Taxation/Taxation_line/Duty_tax_rate    → Tax rate (%)
/ASYCUDA/Item/Valuation_item/Total_CIF_itm            → CIF value
```

### 10.3 CARICOM Preference (CCM)

When `<Preference_code>CCM</Preference_code>` (CARICOM origin):
- CET rate = 0% (duty-free within CARICOM)
- CSC (Customs Service Charge) = 6% still applies
- VAT = 15% still applies

**Track BOTH** the preference rate (0%) AND the standard CET rate from xlsx for non-CARICOM imports.

### 10.4 Classification History Database

```json
{
  "codes": {
    "33049990": {
      "usage_count": 156,
      "cet_rates": [0, 20],
      "descriptions": ["OTHER BEAUTY PREPARATIONS"],
      "preference_codes": ["CCM"],
      "origins": ["TT"],
      "declarations": ["2136059"]
    }
  },
  "corrections": [
    {
      "original": "85167900",
      "corrected": "85437090",
      "product": "ULTRASONIC DIFFUSER",
      "reason": "Ultrasonic ≠ electro-thermic"
    }
  ]
}
```

---

## 11. COMMON ERRORS CATALOG

### Error #1: Cleaning Products in Chapter 33 (MOST COMMON)

| Product | WRONG Code | CORRECT Code |
|---------|------------|--------------|
| Body wash | 33051000 | 34011190 |
| Hand soap (liquid) | 33051000 | 34011190 |
| Dish soap | 33079000 | 34022090 |
| Surface cleaner | 33079000 | 34022090 |
| Baby wipes | 96190000 | 34011190 |
| Laundry detergent | 33079000 | 34022010 |

### Error #2: Using Category Codes Instead of End-Nodes

| Category Used | Product | Correct End-Node |
|--------------|---------|------------------|
| 33049900 | Beauty products | 33049990 |
| 39231000 | Plastic containers | 39231090 |
| 39241000 | Plastic kitchenware | 39241090 or 39249010 |
| 73211100 | Gas stove | 73211110 |
| 84145100 | Ceiling fan | 84145130 |
| 84182100 | Refrigerator | 84182120 |
| 85163000 | Hair dryer | 85163100 |

### Error #3: Gas vs Electric Confusion

Default assumption unless explicitly electric:
- Stove/Range → GAS (73211110)
- Oven → GAS (73211190)

### Error #4: Ultrasonic Products as Electro-thermic

| Product | WRONG | CORRECT |
|---------|-------|---------|
| Ultrasonic diffuser | 85167900 | 85437090 |
| Ultrasonic humidifier | 85167900 | 85437090 |

### Error #5: Services Classified as Products

Services (handling fees, processing charges, etc.) should be placed in Column V (Total Other Cost), NOT as classified product line items with tariff codes.

### Error #6: Swapped Code Pairs

| Product A | Product B | A Code | B Code |
|-----------|-----------|--------|--------|
| Crochet hooks | Safety pins | 73194000 | 73192000 |
| Safety pins | Crochet hooks | 73192000 | 73194000 |

### Error #7: Product Misclassification by Name

| Product | Wrong Chapter | Correct Chapter | Reason |
|---------|--------------|-----------------|--------|
| Barber cape | Ch. 96 (brushes) | Ch. 62/63 (textiles) | It's a textile garment |
| Facial cleanser device | Ch. 85 (hair dryer) | 85094000 | Not a hair dryer |
| Floor mat (rubber) | Ch. 57 (textile) | 40169990 (rubber) | Material determines |

---

## 12. KNOWN INVALID CODES REGISTRY

### Codes That DO NOT EXIST in CARICOM CET

| Invalid Code | Correct Code | Product | Why Invalid |
|-------------|-------------|---------|-------------|
| 96159000 | 96159090 | Hair accessories | Category, not end-node |
| 84145910 | 84145110 | Table fans | Code doesn't exist |
| 85395200 | 85394900 | LED lamps | Code doesn't exist |
| 85167990 | 85167900 | Electro-thermic | Code doesn't exist in CARICOM |
| 65069190 | 65050090 | Headgear | Code doesn't exist |
| 65069290 | 65069100 | Headgear | Code doesn't exist |
| 82121010 | 82121000 | Razors | Code doesn't exist |
| 82121090 | 82122010 | Safety razor blades | Code doesn't exist |
| 33049900 | 33049990 | Beauty preparations | Category (no duty rate) |
| 39269099 | 39269000 | Plastic articles | Code doesn't exist |

**CRITICAL PATTERN:** Adding "90" to category codes doesn't always create valid end-nodes! Always verify in CET xlsx.

### Valid 8516 Electro-Thermic Codes (Verified)

```
85161010 - Electric water heaters ✅
85161020 - Immersion heaters ✅
85162100 - Storage heating radiators ✅
85162900 - Other space/soil heaters ✅
85163100 - Hair dryers ✅
85163200 - Other hair-dressing apparatus ✅
85163300 - Hand-drying apparatus ✅
85164000 - Electric smoothing irons ✅
85165000 - Microwave ovens ✅
85166010 - Stoves and cookers ✅
85166090 - Other ovens, cookers ✅
85167100 - Coffee or tea makers ✅
85167200 - Toasters ✅
85167900 - Other electro-thermic ✅ (this IS valid, but only for HEAT-based)
```

---

## 13. PRODUCT QUICK REFERENCE TABLES

### Chapter 33 — Cosmetics & Personal Care

| Product | Code | Duty |
|---------|------|------|
| Shampoo (HAIR ONLY) | 33051000 | 20% |
| Conditioner | 33051000 | 20% |
| Perfume/EDP | 33030090 | 20% |
| Deodorant (stick/roll-on) | 33072000 | 20% |
| Body spray/cologne | 33079000 | 20% |
| Hair gel/mousse/spray | 33059000 | 20% |
| Lipstick/lip products | 33041000 | 20% |
| Eye makeup (mascara, liner) | 33042000 | 20% |
| Face powder/foundation | 33049100 | 20% |
| Nail polish | 33043000 | 20% |
| Baby powder | 33049990 | 20% |
| Baby lotion | 33049910 | 20% |
| Skincare (CeraVe, etc.) | 33049990 | 20% |
| Body lotion/moisturizer | 33049910 | 20% |
| Hair relaxer | 33059000 | 20% |
| Edge control | 33059000 | 20% |

### Chapter 34 — Soap & Cleaning

| Product | Code | Duty |
|---------|------|------|
| Body wash | 34011190 | 20% |
| Hand soap (liquid/bar) | 34011190 | 20% |
| Baby wipes | 34011190 | 20% |
| Dish soap | 34022090 | 15% |
| Laundry detergent | 34022010 | 15% |
| Surface cleaner | 34022090 | 15% |
| Air freshener | 33074900 | 20% |

### Chapter 38 — Chemical Products

| Product | Code | Duty |
|---------|------|------|
| Disinfectant (Lysol/Dettol) | 38089400 | 5% |
| Insecticide (Raid) | 38089100 | 5% |
| Hand sanitizer | 38089400 | 5% |

### Chapter 67 — Hair Products

| Product | Code | Duty |
|---------|------|------|
| Synthetic hair/braids | 67041900 | 20% |
| Human hair extensions | 67042000 | 20% |
| Wigs (synthetic) | 67041100 | 20% |
| Wigs (human hair) | 67042000 | 20% |

### Chapter 71 — Jewelry

| Product | Code | Duty |
|---------|------|------|
| Imitation jewelry (base metal) | 71171910 | 20% |
| Imitation jewelry (other) | 71179090 | 20% |

### Chapter 73 — Gas Appliances & Iron/Steel

| Product | Code | Duty |
|---------|------|------|
| Gas stove/range | 73211110 | 20% |
| Gas oven | 73211190 | 20% |
| Safety pins | 73192000 | 20% |
| Crochet hooks/knitting needles | 73194000 | 20% |

### Chapter 82 — Tools

| Product | Code | Duty |
|---------|------|------|
| Razors | 82121000 | 20% |
| Safety razor blades | 82122010 | 20% |
| Manicure tools (clippers, files) | 82142000 | 20% |

### Chapter 85 — Electrical Appliances

| Product | Code | Duty |
|---------|------|------|
| Hair dryer | 85163100 | 20% |
| Hair clipper | 85094000 | 20% |
| Blender | 85098010 | 20% |
| Electric stove | 85166010 | 20% |
| Electric kettle | 85167900 | 20% |
| Coffee maker | 85167100 | 20% |
| Toaster | 85167200 | 20% |
| Microwave | 85165000 | 20% |
| TV/Monitor | 85287210 | 5% |
| Table fan | 84145110 | 20% |
| Ceiling fan | 84145130 | 20% |
| Floor/pedestal fan | 84145120 | 20% |
| Ultrasonic diffuser | 85437090 | 20% |
| LED lamps | 85394900 | 20% |
| Facial cleansing device | 85094000 | 20% |

### Chapter 96 — Miscellaneous

| Product | Code | Duty |
|---------|------|------|
| Combs | 96151110 | 20% |
| Hairpins | 96159010 | 20% |
| Other hair accessories | 96159090 | 20% |
| Hair brushes | 96032990 | 20% |
| Toothbrushes | 96032100 | 20% |
| Diapers | 96190012 | 20% |

### Chapter 35 — Adhesives

| Product | Code | Duty |
|---------|------|------|
| Nail glue, wig glue | 35069900 | 5% |

### Chapter 48 — Paper/Cardboard

| Product | Code | Duty |
|---------|------|------|
| Cardboard displays/stands | 48191000 | 20% |

---

## 14. CET XLSX VERIFICATION GUIDE

### 14.1 Which File to Use

| Chapter Range | File |
|---------------|------|
| Chapters 1-30 | `16273revised...link1300.xlsx` |
| Chapters 30-60 | `16273revised...link301600.xlsx` |
| Chapters 60-97 | `16273revised...link601679.xlsx` |

### 14.2 Verification Procedure

```python
import openpyxl
import re

def verify_code_in_cet(code, cet_file):
    """Verify HS code exists as end-node in CET xlsx."""
    wb = openpyxl.load_workbook(cet_file, data_only=True)
    ws = wb.active
    
    # Format code for search: "3304.99.90" or "33049990"
    formatted = f"{code[:4]}.{code[4:6]}.{code[6:8]}"
    
    for row in range(1, ws.max_row + 1):
        a_val = str(ws.cell(row=row, column=1).value or '')
        # Check if this row contains our code
        if code in a_val.replace('.', '') or formatted in a_val:
            # Check for duty rate, unit, and SITC
            has_duty = False
            has_unit = False
            has_sitc = False
            for col in range(1, 15):
                val = str(ws.cell(row=row, column=col).value or '')
                if val and val not in ['None', '']:
                    # Logic to identify duty/unit/sitc columns
                    pass
            return has_duty and has_unit and has_sitc
    
    wb.close()
    return False
```

### 14.3 Red Flags Requiring Investigation

- 🚩 Code ends in 00 (likely category)
- 🚩 Code ends in 000 (definitely category)
- 🚩 No duty rate in CET xlsx
- 🚩 No UNIT column populated
- 🚩 No SITC REV 4 column populated
- 🚩 "Cleaner" or "wash" in Chapter 33
- 🚩 Disinfectant in Chapter 33 or 34
- 🚩 Stove/oven in Chapter 85 without "electric"
- 🚩 Generic "Other" code when specific code exists
- 🚩 Code in Known Invalid Registry (Section 12)

---

## 15. SCRIPT TEMPLATES

### 15.1 Complete PDF-to-Excel Pipeline

```python
#!/usr/bin/env python3
"""
CARICOM Invoice Processing Pipeline
PDF → Classified Excel → Grouped Excel
"""

import pdfplumber
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from collections import OrderedDict
import re

# ─── CONFIGURATION ───
DEFAULTS = {
    'document_type': '7500-000',
    'grouped_doc_type': '4000-000',
    'currency': 'USD',
    'warehouse': '1914',
    'country': 'US'
}

GROUP_FILL = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
GROUP_FONT = Font(bold=True, size=11)
DETAIL_FONT = Font(size=10)
VERIFY_FONT = Font(bold=True, color="0000FF")
VARIANCE_FONT = Font(bold=True, color="FF0000")

HEADERS = [
    'Document Type', 'PO Number', 'Supplier Invoice#', 'Date', 'Category',
    'TariffCode', 'PO Item Number', 'PO Item Description',
    'Supplier Item Number', 'Supplier Item Description', 'Quantity',
    'Per Unit', 'UNITS', 'Currency', 'Cost', 'Total Cost', 'Total',
    'TotalCost Vs Total', 'InvoiceTotal', 'Total Internal Freight',
    'Total Insurance', 'Total Other Cost', 'Total Deduction', 'Packages',
    'Warehouse', 'Supplier Code', 'Supplier Name', 'Supplier Address',
    'Country Code', 'Instructions', 'Previous Declaration',
    'Financial Information', 'Gallons', 'Liters', 'INVTotalCost',
    'POTotalCost', 'GroupBy'
]

# ─── STEP 1: EXTRACT PDF ───
def extract_pdf(pdf_path):
    """Extract all data from invoice PDF."""
    with pdfplumber.open(pdf_path) as pdf:
        text = "\n".join(page.extract_text() or "" for page in pdf.pages)
    
    header = parse_header(text)
    items = parse_items(text)
    return header, items

# ─── STEP 2: CLASSIFY ITEMS ───
def classify_item(description):
    """Classify item using CARICOM HS codes."""
    desc_upper = description.upper()
    
    # High-strength keyword matching
    KEYWORDS = {
        'SHAMPOO': '33051000', 'CONDITIONER': '33051000',
        'PERFUME': '33030090', 'COLOGNE': '33030090',
        'LIPSTICK': '33041000', 'LIP GLOSS': '33041000',
        'MASCARA': '33042000', 'EYELINER': '33042000',
        'FOUNDATION': '33049100', 'FACE POWDER': '33049100',
        'NAIL POLISH': '33043000',
        'DEODORANT': '33072000',
        'BODY WASH': '34011190', 'HAND SOAP': '34011190',
        'DISH SOAP': '34022090', 'DETERGENT': '34022010',
        'DISINFECTANT': '38089400', 'SANITIZER': '38089400',
        'BRAID': '67041900', 'WEAVE': '67041900',
        'WIG': '67041100',
        'COMB': '96151110', 'HAIRPIN': '96159010',
    }
    
    for keyword, code in KEYWORDS.items():
        if keyword in desc_upper:
            return code
    
    # Default fallback — requires manual classification
    return '00000000'

# ─── STEP 3: CREATE EXCEL ───
def create_potemplate(header, items, output_path):
    """Create POTemplate Excel with formulas."""
    wb = Workbook()
    ws = wb.active
    ws.title = "POTemplate"
    
    # Row 1: Headers
    for col, h in enumerate(HEADERS, 1):
        ws.cell(row=1, column=col, value=h)
    
    # Row 2+: Items
    for i, item in enumerate(items):
        row = i + 2
        ws.cell(row=row, column=1, value=DEFAULTS['document_type'] if i == 0 else None)
        ws.cell(row=row, column=3, value=header['invoice_number'])
        ws.cell(row=row, column=4, value=header['date'])
        ws.cell(row=row, column=6, value=item['tariff_code'])
        ws.cell(row=row, column=9, value=item['sku'])
        ws.cell(row=row, column=10, value=item['description'])
        ws.cell(row=row, column=11, value=item['quantity'])
        ws.cell(row=row, column=14, value=DEFAULTS['currency'])
        ws.cell(row=row, column=15, value=item['unit_cost'])
        ws.cell(row=row, column=16, value=item['total_cost'])
        ws.cell(row=row, column=17).value = f'=O{row}*K{row}'
        ws.cell(row=row, column=18).value = f'=P{row}-Q{row}'
        
        # Row 2 reference data
        if i == 0:
            ws.cell(row=2, column=19, value=header['grand_total'])
            ws.cell(row=2, column=20, value=header.get('shipping', 0))
            ws.cell(row=2, column=21, value=header.get('credits', 0))
            ws.cell(row=2, column=22, value=header.get('tax', 0))
            ws.cell(row=2, column=23, value=header.get('discount', 0))
            ws.cell(row=2, column=25, value=DEFAULTS['warehouse'])
            ws.cell(row=2, column=27, value=header['supplier_name'])
            ws.cell(row=2, column=28, value=header.get('supplier_address', ''))
            ws.cell(row=2, column=29, value=header.get('country', DEFAULTS['country']))
    
    # Summary section
    last = len(items) + 1
    sub_row = last + 1
    adj_row = sub_row + 1
    net_row = adj_row + 1
    var_row = net_row + 2
    
    ws.cell(row=sub_row, column=10, value='SUBTOTAL')
    ws.cell(row=sub_row, column=16).value = f'=SUM(P2:P{last})'
    ws.cell(row=sub_row, column=17).value = f'=SUM(Q2:Q{last})'
    
    ws.cell(row=adj_row, column=10, value='ADJUSTMENTS')
    ws.cell(row=adj_row, column=16).value = '=(T2+U2+V2-W2)'
    ws.cell(row=adj_row, column=17).value = '=(T2+U2+V2-W2)'
    
    ws.cell(row=net_row, column=10, value='NET TOTAL')
    ws.cell(row=net_row, column=16).value = f'=(P{sub_row}+P{adj_row})'
    ws.cell(row=net_row, column=17).value = f'=(Q{sub_row}+Q{adj_row})'
    
    ws.cell(row=var_row, column=10, value='VARIANCE CHECK')
    ws.cell(row=var_row, column=16).value = f'=(S2-P{net_row})'
    ws.cell(row=var_row, column=17).value = f'=(S2-Q{net_row})'
    
    wb.save(output_path)
    return output_path

# ─── STEP 4: GROUP BY TARIFF ───
def group_by_tariff(input_path, output_path):
    """Group items by tariff code with group rows at TOP."""
    from openpyxl import load_workbook
    
    wb = load_workbook(input_path)
    ws = wb.active
    
    # Extract reference data from row 2
    ref_data = {}
    for col in range(1, 38):
        ref_data[col] = ws.cell(row=2, column=col).value
    
    # Extract all items
    items = []
    for row in range(2, ws.max_row + 1):
        tariff = ws.cell(row=row, column=6).value
        if not tariff or str(tariff).strip() == '':
            break
        items.append({
            'tariff': str(tariff).strip(),
            'sku': ws.cell(row=row, column=9).value,
            'desc': ws.cell(row=row, column=10).value,
            'qty': ws.cell(row=row, column=11).value or 0,
            'unit_cost': ws.cell(row=row, column=15).value or 0,
            'total_cost': ws.cell(row=row, column=16).value or 0,
        })
    
    # Group by tariff
    grouped = OrderedDict()
    for item in items:
        t = item['tariff']
        if t not in grouped:
            grouped[t] = {'items': [], 'total_qty': 0, 'total_cost': 0.0, 'descs': []}
        grouped[t]['items'].append(item)
        grouped[t]['total_qty'] += item['qty']
        grouped[t]['total_cost'] += item['total_cost']
        grouped[t]['descs'].append(item['desc'])
    
    # Create output
    wb_out = Workbook()
    ws_out = wb_out.active
    ws_out.title = "POTemplate"
    
    # Headers
    for col, h in enumerate(HEADERS, 1):
        ws_out.cell(row=1, column=col, value=h)
    
    current_row = 2
    group_rows = []
    is_first = True
    
    for tariff, grp in grouped.items():
        # Group row
        cat_name = get_category(grp['descs'])
        count = len(grp['items'])
        avg_cost = grp['total_cost'] / grp['total_qty'] if grp['total_qty'] else 0
        
        ws_out.cell(row=current_row, column=1, value='4000-000')
        ws_out.cell(row=current_row, column=3, value=ref_data.get(3))
        ws_out.cell(row=current_row, column=4, value=ref_data.get(4))
        ws_out.cell(row=current_row, column=6, value=tariff)
        ws_out.cell(row=current_row, column=7, value=tariff)
        ws_out.cell(row=current_row, column=8, value=f'{cat_name} ({count} items)')
        ws_out.cell(row=current_row, column=9, value=tariff)
        ws_out.cell(row=current_row, column=10, value=f'{cat_name} ({count} items)')
        ws_out.cell(row=current_row, column=11, value=grp['total_qty'])
        ws_out.cell(row=current_row, column=14, value=ref_data.get(14, 'USD'))
        ws_out.cell(row=current_row, column=15, value=avg_cost)  # FULL PRECISION
        ws_out.cell(row=current_row, column=16, value=grp['total_cost'])
        ws_out.cell(row=current_row, column=17).value = f'=O{current_row}*K{current_row}'
        ws_out.cell(row=current_row, column=18).value = f'=P{current_row}-Q{current_row}'
        ws_out.cell(row=current_row, column=37, value=tariff)  # AK: GroupBy
        
        # First group row gets all reference data
        if is_first:
            for col in [19, 20, 21, 22, 23, 25, 26, 27, 28, 29]:
                ws_out.cell(row=current_row, column=col, value=ref_data.get(col))
            is_first = False
        
        # Format group row
        for col in range(1, 38):
            cell = ws_out.cell(row=current_row, column=col)
            cell.fill = GROUP_FILL
            cell.font = GROUP_FONT
        
        group_rows.append(current_row)
        current_row += 1
        
        # Detail rows
        for item in grp['items']:
            ws_out.cell(row=current_row, column=6, value=tariff)
            ws_out.cell(row=current_row, column=9, value=item['sku'])
            ws_out.cell(row=current_row, column=10, value=f"    {item['desc']}")
            ws_out.cell(row=current_row, column=11, value=item['qty'])
            ws_out.cell(row=current_row, column=15, value=item['unit_cost'])
            ws_out.cell(row=current_row, column=16, value=item['total_cost'])
            ws_out.cell(row=current_row, column=17).value = f'=O{current_row}*K{current_row}'
            ws_out.cell(row=current_row, column=18).value = f'=P{current_row}-Q{current_row}'
            
            for col in range(1, 38):
                ws_out.cell(row=current_row, column=col).font = DETAIL_FONT
            
            current_row += 1
    
    # Totals section
    grp_refs_p = '+'.join([f'P{r}' for r in group_rows])
    grp_refs_q = '+'.join([f'Q{r}' for r in group_rows])
    
    sub_grp = current_row
    ws_out.cell(row=sub_grp, column=10, value='SUBTOTAL (GROUPED)')
    ws_out.cell(row=sub_grp, column=16).value = f'={grp_refs_p}'
    ws_out.cell(row=sub_grp, column=17).value = f'={grp_refs_q}'
    
    sub_det = sub_grp + 1
    ws_out.cell(row=sub_det, column=10, value='SUBTOTAL (DETAILS)')
    # Details = total SUM minus grouped sum
    ws_out.cell(row=sub_det, column=16).value = f'=SUM(P2:P{current_row-1})-{grp_refs_p}'
    ws_out.cell(row=sub_det, column=17).value = f'=SUM(Q2:Q{current_row-1})-{grp_refs_q}'
    
    grp_verify = sub_det + 1
    ws_out.cell(row=grp_verify, column=10, value='GROUP VERIFICATION')
    ws_out.cell(row=grp_verify, column=16).value = f'=P{sub_grp}-P{sub_det}'
    ws_out.cell(row=grp_verify, column=17).value = f'=Q{sub_grp}-Q{sub_det}'
    ws_out.cell(row=grp_verify, column=10).font = VERIFY_FONT
    
    adj_row = grp_verify + 1
    ws_out.cell(row=adj_row, column=10, value='ADJUSTMENTS')
    ws_out.cell(row=adj_row, column=16).value = '=(T2+U2+V2-W2)'
    ws_out.cell(row=adj_row, column=17).value = '=(T2+U2+V2-W2)'
    
    net_row = adj_row + 1
    ws_out.cell(row=net_row, column=10, value='NET TOTAL')
    ws_out.cell(row=net_row, column=16).value = f'=(P{sub_grp}+P{adj_row})'
    ws_out.cell(row=net_row, column=17).value = f'=(Q{sub_grp}+Q{adj_row})'
    
    var_row = net_row + 2
    ws_out.cell(row=var_row, column=10, value='VARIANCE CHECK')
    ws_out.cell(row=var_row, column=16).value = f'=(S2-P{net_row})'
    ws_out.cell(row=var_row, column=17).value = f'=(S2-Q{net_row})'
    ws_out.cell(row=var_row, column=10).font = VARIANCE_FONT
    
    wb_out.save(output_path)
    return output_path

def get_category(descriptions):
    """Generate category name from item descriptions."""
    text = ' '.join([str(d).upper() for d in descriptions if d])
    CATEGORIES = {
        'PERFUMES': ['PERFUME', 'EDP', 'EAU DE PARFUM', 'COLOGNE'],
        'BODY SPRAYS': ['BODY SPRAY', 'BODY MIST'],
        'SHAMPOO & CONDITIONER': ['SHAMPOO', 'CONDITIONER'],
        'BODY WASH & SOAP': ['BODY WASH', 'SOAP', 'CLEANSER'],
        'HAIR CARE': ['HAIR OIL', 'HAIR CREAM', 'HAIR FOOD', 'RELAXER'],
        'SYNTHETIC HAIR': ['BRAID', 'SYNTHETIC', 'WEAVE'],
        'HUMAN HAIR': ['HUMAN HAIR', 'REMI', 'REMY'],
        'COSMETICS': ['LIPSTICK', 'MASCARA', 'MAKEUP', 'EYELINER', 'FOUNDATION'],
        'NAIL PRODUCTS': ['NAIL', 'POLISH', 'MANICURE'],
        'LOTION & CREAM': ['LOTION', 'CREAM', 'MOISTUR'],
        'CLEANING PRODUCTS': ['CLEANER', 'DETERGENT', 'DISH'],
    }
    for cat, keywords in CATEGORIES.items():
        for kw in keywords:
            if kw in text:
                return cat
    return "PRODUCTS"
```

### 15.2 ASYCUDA XML Feedback Processor

```python
#!/usr/bin/env python3
"""Extract tariff data from assessed ASYCUDA XML files."""

import xml.etree.ElementTree as ET
import json
from datetime import datetime

def extract_asycuda_feedback(xml_path):
    tree = ET.parse(xml_path)
    root = tree.getroot()
    
    feedback = {
        'declaration_id': root.get('id'),
        'items': []
    }
    
    for item in root.findall('.//Item'):
        tariff = item.find('.//Tarification')
        if tariff is None:
            continue
        
        hs_code = tariff.find('.//HScode/Commodity_code')
        if hs_code is None or not hs_code.text:
            continue
        
        item_data = {
            'hs_code': hs_code.text,
            'preference_code': (tariff.find('.//Preference_code').text 
                              if tariff.find('.//Preference_code') is not None else None),
            'description': (item.find('.//Goods_description/Description_of_goods').text
                          if item.find('.//Goods_description/Description_of_goods') is not None else ''),
            'commercial_description': (item.find('.//Goods_description/Commercial_Description').text
                                      if item.find('.//Goods_description/Commercial_Description') is not None else ''),
            'taxes': {}
        }
        
        taxation = item.find('.//Taxation')
        if taxation is not None:
            for tax_line in taxation.findall('Taxation_line'):
                tax_code = tax_line.find('Duty_tax_code')
                if tax_code is not None and tax_code.text:
                    item_data['taxes'][tax_code.text] = {
                        'rate': float(tax_line.find('Duty_tax_rate').text or 0),
                        'amount': float(tax_line.find('Duty_tax_amount').text or 0),
                    }
        
        feedback['items'].append(item_data)
    
    return feedback

def update_database(feedback, db_path='classification_history.json'):
    try:
        with open(db_path, 'r') as f:
            db = json.load(f)
    except FileNotFoundError:
        db = {'codes': {}, 'history': []}
    
    for item in feedback['items']:
        code = item['hs_code']
        if code not in db['codes']:
            db['codes'][code] = {'usage_count': 0, 'cet_rates': [], 'descriptions': []}
        
        db['codes'][code]['usage_count'] += 1
        db['codes'][code]['last_used'] = datetime.now().isoformat()
        
        if 'CET' in item['taxes']:
            rate = item['taxes']['CET']['rate']
            if rate not in db['codes'][code]['cet_rates']:
                db['codes'][code]['cet_rates'].append(rate)
    
    with open(db_path, 'w') as f:
        json.dump(db, f, indent=2)
    
    return db
```

---

## 16. APPENDICES

### Appendix A: Chapter Quick Reference

| Chapter | Products |
|---------|----------|
| 33 | Cosmetics, perfumery, hair care (NOT cleaning) |
| 34 | Soap, cleaning preparations, body wash |
| 35 | Adhesives, glues |
| 38 | Chemical products, disinfectants, insecticides |
| 39 | Plastics and plastic articles |
| 40 | Rubber and rubber articles |
| 48 | Paper, cardboard, printed matter |
| 61 | Knitted/crocheted clothing |
| 62 | Woven clothing |
| 63 | Other textile articles |
| 64 | Footwear |
| 65 | Headwear |
| 67 | Human and synthetic hair products |
| 69 | Ceramics |
| 70 | Glass |
| 71 | Jewelry and precious metals |
| 73 | Iron/steel articles, gas appliances |
| 82 | Tools, cutlery |
| 83 | Miscellaneous base metal articles |
| 84 | Machinery (fans, refrigerators, washing machines) |
| 85 | Electrical appliances and equipment |
| 94 | Furniture, bedding, lighting |
| 95 | Toys, games, sports equipment |
| 96 | Miscellaneous manufactured articles (combs, brushes, diapers) |

### Appendix B: Key Definitions

| Term | Definition |
|------|------------|
| End-node | 8-digit code with assigned duty rate, unit, SITC — no subdivisions |
| Category code | Code with subdivisions below, cannot be used for declarations |
| CET | Common External Tariff — CARICOM's tariff schedule |
| POTemplate | Standard Excel format for customs processing |
| Variance | Difference between invoice total and calculated total (must be $0.00) |
| ASYCUDA | Automated SYstem for CUstoms DAta — customs processing software |
| CCM | CARICOM preference code (0% CET for intra-CARICOM trade) |
| CSC | Customs Service Charge (6% in Grenada) |
| CIF | Cost, Insurance, Freight — value used for duty calculation |

### Appendix C: Formula Quick Reference

```excel
# Every data row:
Q = =O{row}*K{row}
R = =P{row}-Q{row}

# Summary (ungrouped):
SUBTOTAL      = =SUM(P2:P{last})
ADJUSTMENTS   = =(T2+U2+V2-W2)
NET TOTAL     = =(P{sub}+P{adj})
VARIANCE      = =(S2-P{net})

# Summary (grouped):
SUB GROUPED   = =P{g1}+P{g2}+P{g3}+...     # Use + not commas
SUB DETAILS   = =SUM(P2:P{last})-{grouped_refs}
GROUP VERIFY  = =P{grp}-P{det}               # Must = $0.00
ADJUSTMENTS   = =(T2+U2+V2-W2)
NET TOTAL     = =(P{grp}+P{adj})
VARIANCE      = =(S2-P{net})                  # Must = $0.00
```

### Appendix D: Recalculation

```bash
# Always run after creating/modifying Excel files with formulas
python /mnt/skills/public/xlsx/recalc.py output.xlsx 60

# Verify output:
# "status": "success", "total_errors": 0
```

### Appendix E: Success Indicators

| Indicator | Target | Source |
|-----------|--------|--------|
| Financial Variance | $0.00 | Every invoice |
| Group Verification | $0.00 | Every grouped file |
| Code Validity | 100% 8-digit end-nodes | CET xlsx verification |
| Formula Errors | 0 | recalc.py output |
| Column Integrity | All 36 columns preserved | Template comparison |

---

## VERSION HISTORY

| Version | Date | Changes |
|---------|------|---------|
| 6.0 | Feb 2026 | Unified master skill combining all 3 skills + 50+ conversation learnings |
| 5.0 | Jan 2026 | Added word analysis (ultrathink) methodology |
| 4.0 | Jan 2026 | Complete rewrite for reusability |
| 3.0 | Nov 2025 | Added ultrathink methodology, major corrections |
| 2.0 | Nov 2025 | Group rows at TOP, classification corrections |
| 1.0 | Nov 2025 | Initial skills package |

---

**END OF CARICOM COMPREHENSIVE SKILL v6.0**
