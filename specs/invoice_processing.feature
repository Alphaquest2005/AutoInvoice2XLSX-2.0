Feature: End-to-end invoice processing
  As a customs broker
  I want to process supplier PDF invoices into classified XLSX worksheets
  So that I can prepare CARICOM customs declarations efficiently

  Background:
    Given a workspace directory is configured
    And the tariff code repository is loaded
    And the classification rules are available

  Scenario: Process a single PDF invoice to XLSX
    Given a PDF invoice from supplier "Acme Corp" with invoice number "INV-2024-001"
    And the invoice contains 5 line items totalling $1,250.00
    When I process the invoice
    Then an XLSX file is generated in the workspace
    And the XLSX contains all 5 items grouped by tariff code
    And the invoice total in the XLSX matches $1,250.00

  Scenario: Process a scanned invoice using OCR fallback
    Given a scanned PDF invoice with no extractable text
    When I process the invoice
    Then the system falls back to OCR extraction
    And the invoice items are successfully parsed from the OCR text
    And the XLSX output is generated with the parsed items

  Scenario: Process an invoice with purchase order matching
    Given a PDF invoice with PO reference "PO-5500"
    And a purchase order "PO-5500" exists in the system
    When I process the invoice
    Then each invoice line is matched against the purchase order
    And the XLSX includes the PO number in the output

  Scenario: Process an invoice containing non-billable items
    Given a PDF invoice with 4 product line items
    And the invoice includes a "Shipping & Handling" charge of $85.00
    And the invoice includes a "Freight" charge of $120.00
    When I process the invoice
    Then only the 4 product items appear as classified detail rows
    And the shipping and freight charges are recorded as non-billable totals
    And the non-billable total equals $205.00

  Scenario: Process a multi-page invoice
    Given a PDF invoice spanning 3 pages
    And the invoice contains 42 line items across all pages
    When I process the invoice
    Then all 42 line items are extracted from the combined pages
    And the XLSX output contains all 42 items grouped by tariff code

  Scenario: Processing fails gracefully when PDF is corrupt
    Given a PDF file that cannot be read
    When I process the invoice
    Then the pipeline reports an error status
    And the error message indicates the PDF could not be extracted
    And no partial XLSX output is generated
