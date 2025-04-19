# Changelog

All notable changes to this project will be documented in this file.

## [1.0.0] - 2025-04-20
### Added
- First version release of the project.
- Implemented core functionality to read data from Excel/CSV, transform and load it to DuckDB.
- Added logging functionality using Dagster logger.
- Supported data type casting for numeric columns and string for pre-processing (before SQL).
- Added a DataFrame preview in the Dagster UI for more convenient debugging and data validation.

### Fixed
- Addressed string to numeric conversion issues in data columns, ensuring that invalid strings (No) are coerced into NaN values.
- Fixed an issue where the database connection was not being properly closed after data load operations.

### Screenshots
Here’s a preview of the application:

![Screenshot](version_image\version1.0.0.PNG)

### Notes
- This release includes all the necessary functionality for data extraction and loading.
- The image above shows a sample preview of the output from the application.

