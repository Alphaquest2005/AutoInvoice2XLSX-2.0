Feature: CET tariff code validation
  As a customs broker
  I want all tariff codes validated against the CARICOM CET schedule
  So that only valid 8-digit end-node codes appear on customs declarations

  Background:
    Given the tariff code repository is loaded with the current CET schedule
    And the invalid codes registry is loaded from invalid_codes.json

  Scenario: Valid 8-digit code passes validation
    Given a tariff code "84713000"
    When the code is validated
    Then the validation passes
    And the code is accepted as a valid CET tariff code

  Scenario: Non-8-digit code is rejected
    Given a tariff code "8471"
    When the code is validated
    Then the validation fails
    And the error indicates the code must be exactly 8 digits

  Scenario: Category heading code is rejected as non-end-node
    Given a tariff code "84710000"
    And code "84710000" is a category heading in the CET schedule, not an end-node
    When the code is validated
    Then the validation fails
    And the error indicates the code is a heading, not a classifiable end-node

  Scenario: Known bad code is corrected from invalid codes registry
    Given a tariff code "96034000"
    And the invalid codes registry maps "96034000" to "96034020"
    When the code is validated
    Then the original code is flagged as invalid
    And the corrected code "96034020" is returned
    And a correction record is logged

  Scenario: Extract chapter and heading from a valid code
    Given a tariff code "22021010"
    When the chapter and heading are extracted
    Then the chapter is "22"
    And the heading is "2202"
    And the subheading is "220210"

  Scenario: Non-numeric code is rejected
    Given a tariff code "8471A000"
    When the code is validated
    Then the validation fails
    And the error indicates the code must contain only digits
