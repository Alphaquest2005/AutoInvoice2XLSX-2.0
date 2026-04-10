Feature: Variance verification between invoice total and XLSX total
  As a customs broker
  I want the system to verify that the XLSX output total matches the original invoice total
  So that I can be confident no line items were lost or miscalculated

  Background:
    Given a processed invoice with a known invoice total
    And an XLSX output has been generated from the classified items

  Scenario: Zero variance passes verification
    Given the invoice total is $1,250.00
    And the XLSX computed total is $1,250.00
    When the variance check runs
    Then the variance is $0.00
    And the verification passes

  Scenario: Non-zero variance beyond threshold triggers error
    Given the invoice total is $1,250.00
    And the XLSX computed total is $1,245.00
    When the variance check runs
    Then the variance is $5.00
    And the verification fails with a variance error
    And the error includes both the invoice total and the computed total

  Scenario: Variance within threshold passes
    Given the invoice total is $1,250.00
    And the XLSX computed total is $1,249.75
    And the variance threshold is $0.50
    When the variance check runs
    Then the variance is $0.25
    And the verification passes because the variance is within the threshold

  Scenario: LLM variance fix adjusts freight and insurance allocations
    Given the invoice total is $1,250.00
    And the XLSX computed total is $1,247.50
    And the variance of $2.50 exceeds the threshold
    When the LLM variance fixer is invoked
    Then the fixer adjusts freight or insurance allocations to close the gap
    And the adjusted XLSX total matches the invoice total within the threshold

  Scenario: Maximum retry attempts are respected
    Given the invoice total is $1,250.00
    And the XLSX computed total is $1,200.00
    And the maximum LLM variance fix attempts is 2
    When the LLM variance fixer fails to resolve the variance after 2 attempts
    Then the system stops retrying
    And the pipeline reports a variance error with the remaining difference
    And the XLSX is marked as unverified
