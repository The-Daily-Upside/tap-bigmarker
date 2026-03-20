# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [0.1.0] - 2026-03-20

### Added
- Expanded webinar-focused endpoint coverage across channel, conference, reporting, registration, automation, recording, and analytics APIs.
- Added optional tap config settings for endpoint-specific inputs and filters:
  - `download_bmid`
  - `custom_email_id`
  - `meeting_space_id`
  - `integration_type`
  - `registered_conferences_email`
  - `channel_block_list_type`
  - `registration_block_list_type`
  - `custom_email_action_type`
  - `custom_email_date_start`
  - `custom_email_date_end`

### Changed
- Standardized records to a raw envelope model (`id`, context keys, `raw`) for broad schema-compatible ingestion.
- Improved parent/child context propagation so child streams use source IDs reliably.
- Hardened pagination termination logic to prevent runaway paging.
- Added semantic optional-resource handling for known non-fatal `404` responses.
- Added selective stream-level `429` tolerance for high-friction optional endpoints to avoid blocking full extraction runs.
- Updated README with required/optional configuration and operational behavior notes.
