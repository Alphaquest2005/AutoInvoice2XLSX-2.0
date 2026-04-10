Feature: Email integration for invoice intake and output delivery
  As a customs broker
  I want to receive invoice PDFs via email and send completed XLSX files to recipients
  So that the invoice processing workflow integrates with my existing email-based processes

  Background:
    Given the email gateway is configured with SMTP credentials
    And the recipient list is configured

  Scenario: Process an email with an invoice PDF attachment
    Given an incoming email with subject "Invoice for Shipment BL-2024-0055"
    And the email has a PDF attachment named "invoice_5500.pdf"
    When the email is processed
    Then the PDF attachment is extracted to the workspace
    And the invoice processing pipeline runs on the extracted PDF
    And the generated XLSX is associated with the email

  Scenario: Split a combined PDF containing BL and invoice
    Given an incoming email with a single PDF attachment
    And the PDF contains a Bill of Lading on pages 1-2 and an invoice on pages 3-5
    When the email is processed
    Then the PDF is split into separate BL and invoice documents
    And the invoice portion is processed through the classification pipeline
    And the BL portion is parsed for shipment metadata

  Scenario: Extract waybill number from the email subject line
    Given an incoming email with subject "Docs for AWB 176-43298765"
    When the email subject is parsed
    Then the waybill number "176-43298765" is extracted
    And the waybill number is associated with the shipment output

  Scenario: Send completed XLSX to configured recipients
    Given a successfully processed invoice with verified variance
    And the output XLSX file "INV-5500_classified.xlsx" is ready
    When the output is emailed
    Then an email is sent to all configured recipients
    And the XLSX file is included as an attachment
    And the email subject contains the invoice number

  Scenario: Skip email send when variance check fails
    Given a processed invoice where the variance check failed
    And the XLSX is marked as unverified
    When the output delivery step runs
    Then no email is sent to the recipients
    And the pipeline report notes that email delivery was skipped due to variance failure
