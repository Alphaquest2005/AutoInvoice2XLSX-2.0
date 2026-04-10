Feature: Invoice format detection and parsing
  As a customs broker
  I want the system to automatically detect the supplier invoice format
  So that each PDF is parsed using the correct field extraction rules

  Background:
    Given the format registry is loaded from YAML configuration files
    And a default fallback format is defined

  Scenario: Auto-detect a known supplier format
    Given a PDF invoice from supplier "Sysco International"
    And the format registry contains a YAML spec for "sysco_international"
    When the invoice format is detected
    Then the format "sysco_international" is selected
    And the invoice is parsed using the Sysco field mappings

  Scenario: Fall back to default format for unknown supplier
    Given a PDF invoice from an unrecognized supplier
    And no format in the registry matches the invoice layout
    When the invoice format is detected
    Then the default format is selected
    And the invoice is parsed using generic field extraction rules

  Scenario: Generate a format spec using LLM for a new supplier
    Given a PDF invoice from a new supplier "Pacific Trading Co"
    And no format in the registry matches the invoice layout
    And the user requests LLM-assisted format generation
    When the LLM analyzes the invoice text
    Then a new format specification is generated for "pacific_trading_co"
    And the spec includes field positions for invoice number, date, and line items
    And the generated spec is saved to the format registry

  Scenario: Load format specifications from YAML files
    Given the formats directory contains 3 YAML format specification files
    When the format registry is loaded
    Then 3 supplier formats are available for detection
    And each format includes supplier name, field mappings, and line item patterns

  Scenario: Detect format by matching header patterns
    Given a PDF invoice with header text containing "COMMERCIAL INVOICE" and "Sysco International"
    And the "sysco_international" format spec defines header pattern "Sysco International"
    When the format detection scans the extracted text
    Then the "sysco_international" format is matched by its header pattern
    And the confidence of the match is recorded
