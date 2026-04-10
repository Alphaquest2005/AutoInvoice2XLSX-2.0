Feature: ASYCUDA classification data import
  As a customs broker
  I want to import assessed tariff classifications from ASYCUDA XML exports
  So that previously verified classifications are reused for future invoices

  Background:
    Given the classification store is initialized
    And the tariff code repository is loaded for validation

  Scenario: Import classification data from ASYCUDA XML
    Given an ASYCUDA XML export file containing 50 classification records
    And each record includes an SKU, tariff code, and item description
    When the ASYCUDA data is imported
    Then 50 assessed classifications are stored in the database
    And each classification is marked with source "asycuda"

  Scenario: Update existing assessed classifications
    Given SKU "WH-4420" has an existing assessed classification of "84713000"
    And the ASYCUDA XML contains an updated classification of "84714100" for SKU "WH-4420"
    When the ASYCUDA data is imported
    Then the classification for SKU "WH-4420" is updated to "84714100"
    And a correction record is saved from "84713000" to "84714100"

  Scenario: Track SKU corrections during import
    Given the ASYCUDA XML contains a record for SKU "OF-1100" with code "96081010"
    And the system previously classified SKU "OF-1100" as "96081099" via rules
    When the ASYCUDA data is imported
    Then a correction is recorded from "96081099" to "96081010" for SKU "OF-1100"
    And future classifications for SKU "OF-1100" use the assessed code "96081010"

  Scenario: Detect and skip duplicate records during import
    Given the ASYCUDA XML contains 2 records for SKU "EL-3300" with the same code "85044090"
    When the ASYCUDA data is imported
    Then only one classification is stored for SKU "EL-3300"
    And the import summary reports 1 duplicate skipped

  Scenario: Reject records with invalid tariff codes
    Given the ASYCUDA XML contains a record for SKU "BK-0010" with code "1234"
    When the ASYCUDA data is imported
    Then the record for SKU "BK-0010" is rejected
    And the import summary reports 1 invalid code
    And no classification is stored for SKU "BK-0010"
