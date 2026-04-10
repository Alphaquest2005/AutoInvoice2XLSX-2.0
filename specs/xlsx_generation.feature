Feature: XLSX output generation
  As a customs broker
  I want the classified invoice data written to a structured XLSX worksheet
  So that it conforms to the standard 37-column customs declaration format

  Background:
    Given a grouped invoice with items classified by tariff code
    And the XLSX column specification is loaded with 37 columns

  Scenario: Generate XLSX with correct column structure
    Given a grouped invoice from supplier "Global Supplies Ltd"
    When the XLSX is generated
    Then the worksheet contains exactly 37 columns
    And the header row contains the correct column names
    And the sheet name matches the expected output name

  Scenario: Apply group header and detail row styling
    Given a tariff group "39241090" with 3 detail items
    When the XLSX is generated
    Then the group header row has bold text and a shaded background
    And the 3 detail rows use the standard detail style
    And the group header row precedes its detail rows

  Scenario: Place tariff code on group rows only
    Given a tariff group "84713000" with 2 detail items
    When the XLSX is generated
    Then the tariff code "84713000" appears on the group header row
    And the tariff code cell is empty on both detail rows

  Scenario: Generate correct formulas for subtotals and grand totals
    Given 2 tariff groups with total values $500.00 and $750.00
    When the XLSX is generated
    Then each group has a subtotal row with a SUM formula over its detail rows
    And the grand total row contains a formula summing all group subtotals
    And the grand total evaluates to $1,250.00

  Scenario: Generate multi-invoice sections with subtotals
    Given 2 invoices from the same shipment
    And invoice "INV-001" has a total of $800.00
    And invoice "INV-002" has a total of $450.00
    When the XLSX is generated
    Then each invoice occupies its own section in the worksheet
    And each section has a subtotal row
    And a combined grand total row shows $1,250.00

  Scenario: Format currency values correctly
    Given a detail row with unit cost $12.3456 and total cost $49.3824
    When the XLSX is generated
    Then currency cells display values with 2 decimal places
    And currency cells use the number format "#,##0.00"
