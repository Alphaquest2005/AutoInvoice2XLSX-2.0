Feature: Item classification with CET tariff codes
  As a customs broker
  I want invoice items automatically classified with 8-digit CET tariff codes
  So that each item has the correct code for the CARICOM customs declaration

  Background:
    Given the tariff code repository is loaded
    And the classification rules contain keyword mappings
    And the ASYCUDA assessed classifications database is available

  Scenario: Classify an item using rule-based keyword matching
    Given an invoice item with description "Ballpoint Pen, Blue Ink, Box of 12"
    And a classification rule maps keyword "ballpoint pen" to tariff code "96081010"
    When the item is classified
    Then the assigned tariff code is "96081010"
    And the classification source is "rules"
    And the confidence score is 1.0

  Scenario: Classify an item using ASYCUDA assessed classification
    Given an invoice item with SKU "WH-4420"
    And SKU "WH-4420" has an assessed classification of "84713000" in the ASYCUDA database
    When the item is classified
    Then the assigned tariff code is "84713000"
    And the classification source is "assessed"
    And the confidence score is 1.0

  Scenario: Auto-correct an invalid tariff code
    Given an invoice item classified with code "96034000"
    And code "96034000" is listed in the invalid codes registry with correction "96034020"
    When the classification is validated
    Then the tariff code is corrected to "96034020"
    And a correction record is saved linking "96034000" to "96034020"

  Scenario: Fall back to LLM classification for unmatched items
    Given an invoice item with description "Ergonomic Lumbar Support Cushion"
    And no classification rule matches the item description
    And no assessed classification exists for the item SKU
    When the item is classified
    Then the LLM classifier is invoked with the item description
    And the classification source is "llm"
    And the confidence score is less than 1.0

  Scenario: Assign category based on tariff chapter
    Given an invoice item classified with tariff code "22021010"
    When the category is determined
    Then the item is assigned to the "BEVERAGES" category
    And the category appears on the group header row in the output

  Scenario: Exclusion pattern prevents false keyword match
    Given an invoice item with description "Printer Ink Cartridge, Black"
    And a classification rule maps keyword "ink" to tariff code "32151100"
    But an exclusion pattern excludes "ink cartridge" from that rule
    When the item is classified
    Then the item is not classified by the "ink" keyword rule
    And the classifier proceeds to the next classification strategy
