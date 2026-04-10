Feature: Bill of Lading based shipment batch processing
  As a customs broker
  I want to process an entire shipment folder containing a BL, invoices, and purchase orders
  So that I can generate a combined customs declaration for the whole shipment

  Background:
    Given a shipment workspace directory exists
    And the tariff code repository is loaded

  Scenario: Process a shipment folder with BL and invoices
    Given a shipment folder containing:
      | file                  | type    |
      | BL_2024-0055.pdf      | bl      |
      | invoice_5500.pdf      | invoice |
      | invoice_5501.pdf      | invoice |
      | PO-8800.pdf           | po      |
    When the shipment is processed
    Then both invoices are parsed and classified
    And a combined XLSX output is generated for the shipment
    And the BL number "2024-0055" appears in the output header

  Scenario: Extract BL metadata for the shipment
    Given a Bill of Lading PDF for BL number "2024-0055"
    And the BL lists consignee "Caribbean Imports Ltd"
    And the BL shows 15 packages weighing 420.5 kg
    When the BL is parsed
    Then the consignee name is "Caribbean Imports Ltd"
    And the total packages extracted is 15
    And the total weight extracted is 420.5 kg

  Scenario: Allocate packages from BL across invoices
    Given a BL with 15 total packages
    And invoice "INV-5500" covers 3 tariff groups with 10 items
    And invoice "INV-5501" covers 2 tariff groups with 5 items
    When package allocation runs
    Then packages are distributed proportionally across the tariff groups
    And the total allocated packages across all groups equals 15

  Scenario: Generate combined shipment output
    Given a shipment with BL "2024-0055"
    And 2 processed invoices with a combined total of $3,200.00
    When the combined output is generated
    Then a single XLSX file is created for the shipment
    And the XLSX contains separate sections for each invoice
    And the shipment grand total equals $3,200.00
    And the BL metadata is included in the output header
