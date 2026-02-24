_____________________________________________
## *Author*: AAVA
## *Created on*:   
## *Description*: Functional test cases for adding SOURCE_TILE_METADATA table and extending target summary table with tile category
## *Version*: 1 
## *Updated on*: 
_____________________________________________

# Functional Test Cases for PCE-5: Add New Source Table SOURCE_TILE_METADATA and Extend Target Summary Table with Tile Category Business Request

## Overview
This document contains comprehensive functional test cases for implementing the SOURCE_TILE_METADATA table and extending the target summary table with tile category functionality as specified in Jira story PCE-5.

## Test Cases

### Test Case ID: TC_PCE5_01
**Title:** Validate SOURCE_TILE_METADATA table creation with correct schema
**Description:** Ensure that the SOURCE_TILE_METADATA table is created successfully in analytics_db with the correct schema structure and properties.
**Preconditions:**
- Database analytics_db exists and is accessible
- User has CREATE TABLE permissions
- Delta Lake is properly configured

**Steps to Execute:**
1. Execute the CREATE TABLE statement for analytics_db.SOURCE_TILE_METADATA
2. Verify table exists in analytics_db catalog
3. Check table schema matches expected structure
4. Validate table is using DELTA format
5. Verify table comment is properly set

**Expected Result:**
- Table analytics_db.SOURCE_TILE_METADATA is created successfully
- Schema contains: tile_id (STRING), tile_name (STRING), tile_category (STRING), is_active (BOOLEAN), updated_ts (TIMESTAMP)
- All columns have appropriate comments
- Table uses DELTA format
- Table comment matches specification

**Linked Jira Ticket:** PCE-5

### Test Case ID: TC_PCE5_02
**Title:** Validate successful insertion of sample metadata records
**Description:** Ensure that sample tile metadata can be inserted into SOURCE_TILE_METADATA table without errors.
**Preconditions:**
- SOURCE_TILE_METADATA table exists
- User has INSERT permissions

**Steps to Execute:**
1. Insert sample records with various tile categories (e.g., "Personal Finance", "Health", "Payments")
2. Insert records with different is_active values (true/false)
3. Insert records with current timestamps
4. Verify record count matches inserted records
5. Query table to validate data integrity

**Expected Result:**
- All sample records are inserted successfully
- No data type conversion errors occur
- Record count matches expected number
- All fields contain expected values
- Timestamps are properly formatted

**Linked Jira Ticket:** PCE-5

### Test Case ID: TC_PCE5_03
**Title:** Validate ETL pipeline reads SOURCE_TILE_METADATA table successfully
**Description:** Ensure that the ETL pipeline can successfully read from the SOURCE_TILE_METADATA table without errors.
**Preconditions:**
- SOURCE_TILE_METADATA table exists with sample data
- ETL pipeline is configured to access the table
- Necessary permissions are granted

**Steps to Execute:**
1. Execute ETL pipeline that reads from SOURCE_TILE_METADATA
2. Verify connection to analytics_db is established
3. Check that all records are read successfully
4. Validate schema inference works correctly
5. Confirm no connection or permission errors

**Expected Result:**
- ETL pipeline connects to SOURCE_TILE_METADATA successfully
- All records are read without data loss
- Schema is correctly inferred
- No connection timeouts or permission errors
- Pipeline execution completes successfully

**Linked Jira Ticket:** PCE-5

### Test Case ID: TC_PCE5_04
**Title:** Validate tile_category column addition to target table
**Description:** Ensure that the tile_category column is successfully added to the target summary table with correct data type and comment.
**Preconditions:**
- Target summary table exists
- User has ALTER TABLE permissions
- ETL pipeline is updated to include tile_category

**Steps to Execute:**
1. Execute ALTER TABLE statement to add tile_category column
2. Verify column is added with STRING data type
3. Check column comment is set correctly
4. Validate table schema includes new column
5. Ensure existing data remains intact

**Expected Result:**
- tile_category column is added successfully
- Column has STRING data type
- Column comment reads "Functional category of the tile"
- Existing table data is preserved
- No schema drift errors occur

**Linked Jira Ticket:** PCE-5

### Test Case ID: TC_PCE5_05
**Title:** Validate LEFT JOIN between existing data and SOURCE_TILE_METADATA
**Description:** Ensure that the ETL transformation correctly performs LEFT JOIN to enrich existing tile data with metadata.
**Preconditions:**
- SOURCE_TILE_METADATA table contains test data
- Existing tile data table has tile_id column
- ETL transformation is updated with LEFT JOIN logic

**Steps to Execute:**
1. Execute ETL transformation with LEFT JOIN on tile_id
2. Verify join matches records correctly
3. Check that unmatched records are preserved
4. Validate tile_category is populated for matched records
5. Confirm record count remains consistent

**Expected Result:**
- LEFT JOIN executes successfully
- Matched records show correct tile_category values
- Unmatched records are preserved with NULL tile_category
- Total record count matches original data
- No duplicate records are created

**Linked Jira Ticket:** PCE-5

### Test Case ID: TC_PCE5_06
**Title:** Validate backward compatibility with default "UNKNOWN" category
**Description:** Ensure that tiles without metadata mapping default to "UNKNOWN" category to maintain backward compatibility.
**Preconditions:**
- SOURCE_TILE_METADATA table exists but doesn't contain all tile_ids
- ETL pipeline includes default value logic
- Target table accepts "UNKNOWN" as valid category

**Steps to Execute:**
1. Process tiles that exist in main data but not in SOURCE_TILE_METADATA
2. Verify ETL assigns "UNKNOWN" to tile_category for unmatched records
3. Check that existing tile metrics are preserved
4. Validate no records are dropped due to missing metadata
5. Confirm "UNKNOWN" appears in final output

**Expected Result:**
- Unmatched tiles receive "UNKNOWN" as tile_category
- All original tile records are preserved
- No data loss occurs for tiles without metadata
- "UNKNOWN" category appears correctly in reporting outputs
- Backward compatibility is maintained

**Linked Jira Ticket:** PCE-5

### Test Case ID: TC_PCE5_07
**Title:** Validate target table writing logic includes tile_category
**Description:** Ensure that the updated target table writing logic correctly includes tile_category in SELECT statements and partition logic.
**Preconditions:**
- Target table schema includes tile_category column
- ETL pipeline is updated with new writing logic
- Test data includes various tile categories

**Steps to Execute:**
1. Execute ETL pipeline with updated writing logic
2. Verify SELECT statement includes tile_category column
3. Check overwritePartition logic handles tile_category correctly
4. Validate data is written to target table successfully
5. Confirm tile_category values appear in final output

**Expected Result:**
- tile_category is included in SELECT statement
- overwritePartition logic works with new column
- Data is written successfully to target table
- tile_category values are correctly populated
- No schema validation errors occur

**Linked Jira Ticket:** PCE-5

### Test Case ID: TC_PCE5_08
**Title:** Validate schema validation in unit tests passes
**Description:** Ensure that updated unit tests pass schema validation with the new tile_category column.
**Preconditions:**
- Unit tests are updated to include tile_category
- Test data includes tile_category values
- Schema validation logic is updated

**Steps to Execute:**
1. Run unit tests with updated schema validation
2. Verify tests pass with tile_category column present
3. Check tests fail appropriately when tile_category is missing
4. Validate data type validation works for tile_category
5. Confirm schema drift detection works correctly

**Expected Result:**
- Unit tests pass when tile_category is present
- Tests fail when tile_category is missing or wrong type
- Schema validation correctly identifies tile_category as STRING
- No false positives in schema drift detection
- All test assertions pass successfully

**Linked Jira Ticket:** PCE-5

### Test Case ID: TC_PCE5_09
**Title:** Validate correct record counts are maintained
**Description:** Ensure that the ETL pipeline maintains correct record counts after adding tile_category enrichment.
**Preconditions:**
- Baseline record counts are established
- ETL pipeline includes tile_category enrichment
- Test data has known record counts

**Steps to Execute:**
1. Record baseline counts before ETL execution
2. Execute ETL pipeline with tile_category enrichment
3. Count records in target table after processing
4. Compare before and after record counts
5. Verify no records are lost or duplicated

**Expected Result:**
- Record counts match between input and output
- No records are lost during enrichment process
- No duplicate records are created
- Count validation passes in unit tests
- Data integrity is maintained throughout pipeline

**Linked Jira Ticket:** PCE-5

### Test Case ID: TC_PCE5_10
**Title:** Validate tile_category enrichment accuracy
**Description:** Ensure that tile_category values are accurately mapped and appear correctly in reporting outputs.
**Preconditions:**
- SOURCE_TILE_METADATA contains accurate test mappings
- ETL pipeline processes tile_category correctly
- Reporting outputs include tile_category field

**Steps to Execute:**
1. Create test data with known tile_id to tile_category mappings
2. Execute complete ETL pipeline
3. Verify tile_category values in final output match expected mappings
4. Check that different categories are correctly assigned
5. Validate reporting outputs show accurate categorization

**Expected Result:**
- tile_category values match expected mappings exactly
- Different tile categories are correctly distinguished
- Reporting outputs show accurate tile categorization
- No incorrect category assignments occur
- Data lineage from source to target is maintained

**Linked Jira Ticket:** PCE-5

### Test Case ID: TC_PCE5_11
**Title:** Validate pipeline runs without schema drift errors
**Description:** Ensure that the complete ETL pipeline runs successfully without any schema drift errors after implementing tile_category changes.
**Preconditions:**
- All schema changes are implemented
- Pipeline includes schema validation checks
- Target systems expect new schema

**Steps to Execute:**
1. Execute complete ETL pipeline end-to-end
2. Monitor for any schema drift warnings or errors
3. Verify schema evolution is handled correctly
4. Check that downstream systems accept new schema
5. Validate pipeline completes successfully

**Expected Result:**
- Pipeline executes without schema drift errors
- Schema evolution is handled gracefully
- Downstream systems accept updated schema
- No compatibility issues arise
- Pipeline completion status is successful

**Linked Jira Ticket:** PCE-5

### Test Case ID: TC_PCE5_12
**Title:** Validate category-level performance metrics reporting
**Description:** Ensure that business teams can view performance metrics by tile category as specified in requirements.
**Preconditions:**
- ETL pipeline has processed data with tile_category
- Reporting system can access enriched data
- Test data includes multiple categories

**Steps to Execute:**
1. Query enriched data grouped by tile_category
2. Calculate performance metrics (views, clicks, CTRs) by category
3. Verify metrics can be compared across categories
4. Check that "Personal Finance" vs "Health" comparisons work
5. Validate category-level aggregations are accurate

**Expected Result:**
- Performance metrics can be grouped by tile_category
- Category-level comparisons are possible
- Aggregations (SUM, AVG, COUNT) work correctly by category
- Business users can distinguish between tile types
- Reporting queries execute successfully

**Linked Jira Ticket:** PCE-5

### Test Case ID: TC_PCE5_13
**Title:** Validate inactive tile handling
**Description:** Ensure that tiles marked as inactive (is_active = false) in SOURCE_TILE_METADATA are handled correctly.
**Preconditions:**
- SOURCE_TILE_METADATA contains tiles with is_active = false
- ETL pipeline processes is_active flag appropriately
- Business rules for inactive tiles are defined

**Steps to Execute:**
1. Include tiles with is_active = false in SOURCE_TILE_METADATA
2. Execute ETL pipeline
3. Verify inactive tiles are still enriched with tile_category
4. Check that is_active status doesn't affect category assignment
5. Validate reporting can filter by active/inactive status

**Expected Result:**
- Inactive tiles receive correct tile_category values
- is_active flag doesn't interfere with category enrichment
- Reporting can filter by tile status if needed
- Data completeness is maintained for all tiles
- Business rules for inactive tiles are followed

**Linked Jira Ticket:** PCE-5

### Test Case ID: TC_PCE5_14
**Title:** Validate error handling for malformed metadata
**Description:** Ensure that the ETL pipeline handles malformed or invalid data in SOURCE_TILE_METADATA gracefully.
**Preconditions:**
- SOURCE_TILE_METADATA contains some invalid records
- ETL pipeline includes error handling logic
- Data quality rules are defined

**Steps to Execute:**
1. Insert records with NULL tile_id in SOURCE_TILE_METADATA
2. Insert records with empty tile_category values
3. Execute ETL pipeline
4. Verify pipeline handles invalid data gracefully
5. Check that valid records are still processed correctly

**Expected Result:**
- Pipeline doesn't fail due to malformed metadata
- Invalid records are logged or filtered appropriately
- Valid records continue to be processed correctly
- Error handling logs provide useful information
- Data quality is maintained despite bad input data

**Linked Jira Ticket:** PCE-5

### Test Case ID: TC_PCE5_15
**Title:** Validate performance impact of metadata enrichment
**Description:** Ensure that adding tile_category enrichment doesn't significantly impact ETL pipeline performance.
**Preconditions:**
- Baseline performance metrics are established
- ETL pipeline includes tile_category enrichment
- Performance monitoring is in place

**Steps to Execute:**
1. Measure ETL execution time before tile_category implementation
2. Execute ETL pipeline with tile_category enrichment
3. Measure execution time after implementation
4. Compare performance metrics
5. Verify performance impact is within acceptable limits

**Expected Result:**
- Performance impact is minimal (< 10% increase in execution time)
- Memory usage remains within acceptable limits
- JOIN operation doesn't cause significant slowdown
- Pipeline can handle expected data volumes
- Performance meets SLA requirements

**Linked Jira Ticket:** PCE-5

## Edge Cases and Boundary Conditions

### Test Case ID: TC_PCE5_16
**Title:** Validate handling of duplicate tile_id in SOURCE_TILE_METADATA
**Description:** Ensure that duplicate tile_id entries in SOURCE_TILE_METADATA are handled appropriately.
**Preconditions:**
- SOURCE_TILE_METADATA contains duplicate tile_id values
- ETL pipeline includes duplicate handling logic

**Steps to Execute:**
1. Insert multiple records with same tile_id but different tile_category
2. Execute ETL pipeline
3. Verify how duplicates are resolved (first match, last match, or error)
4. Check that pipeline doesn't fail due to duplicates
5. Validate final output has consistent tile_category per tile_id

**Expected Result:**
- Duplicate handling follows defined business rules
- Pipeline executes successfully despite duplicates
- Final output has one tile_category per tile_id
- Duplicate resolution is logged appropriately
- Data consistency is maintained

**Linked Jira Ticket:** PCE-5

### Test Case ID: TC_PCE5_17
**Title:** Validate handling of very long tile_category names
**Description:** Ensure that extremely long tile_category values don't cause truncation or errors.
**Preconditions:**
- SOURCE_TILE_METADATA contains tile_category with 1000+ characters
- Target table can accommodate long strings

**Steps to Execute:**
1. Insert record with very long tile_category (1000+ chars)
2. Execute ETL pipeline
3. Verify long category name is preserved
4. Check no truncation occurs in target table
5. Validate reporting systems can handle long names

**Expected Result:**
- Long tile_category values are preserved completely
- No truncation or data loss occurs
- Target table accommodates full-length values
- Reporting systems display long names correctly
- No buffer overflow or string handling errors

**Linked Jira Ticket:** PCE-5

### Test Case ID: TC_PCE5_18
**Title:** Validate handling of special characters in tile_category
**Description:** Ensure that special characters, Unicode, and emojis in tile_category are handled correctly.
**Preconditions:**
- SOURCE_TILE_METADATA contains tile_category with special characters
- System supports Unicode encoding

**Steps to Execute:**
1. Insert records with tile_category containing emojis, Unicode, special chars
2. Execute ETL pipeline
3. Verify special characters are preserved
4. Check encoding/decoding works correctly
5. Validate reporting displays special characters properly

**Expected Result:**
- Special characters are preserved throughout pipeline
- Unicode encoding/decoding works correctly
- Emojis and symbols display properly in reports
- No character corruption occurs
- International characters are supported

**Linked Jira Ticket:** PCE-5

## Summary
This test suite provides comprehensive coverage for the SOURCE_TILE_METADATA implementation including:
- Table creation and schema validation
- Data insertion and retrieval
- ETL pipeline integration
- JOIN operations and data enrichment
- Backward compatibility
- Error handling and edge cases
- Performance validation
- Reporting functionality

All test cases are designed to ensure the successful implementation of tile category functionality while maintaining data integrity and system performance.