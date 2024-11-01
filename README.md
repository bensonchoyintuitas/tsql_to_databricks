# Introduction 
Converts TSQL dbt or sql models to ANSI SQL in style of databricks.


# Written by:
- [@benson-choy](https://github.com/bensonchoyintuitas/tsql_to_databricks)
- Master branch is protected. Create a branch and pull request to merge into master...

# To run:

```bash
# For a single file
python3 convert_tsql_to_databricks.py input.sql output.sql
```
```bash
# For a whole folder and subfolders (place models into the input 
python3 convert_folder_tsql_to_databricks_ansi.py ./input ./output
```

# Areas for improvement:
1. Apply SQL Linting to the output.
2. Include more dbt header config types.
3. Provide regression test suite

# Known Gaps:
1. DATEDIFF(second,`Departure_Actual_At`,`AdmissionDate`) may require manual handling as DATABRICKS DATEDIFF defaults to minutes and not seconds. Automation of this resol

2. AS PRESENT_AGE placed in the wrong position:
```sql
     -- Input:

          ,PRESENT_AGE = FLOOR(DATEDIFF(DAY, E.PRESENT_DOB, ISNULL(CASE
                                                                 WHEN TDT.TRIAGED_AT_AEST IS NOT NULL
                                                                      AND E.QUICK_REGISTRATION_AEST IS NULL
                                                                 THEN TDT.TRIAGED_AT_AEST
                                                                 WHEN TDT.TRIAGED_AT_AEST < E.QUICK_REGISTRATION_AEST
                                                                 THEN TDT.TRIAGED_AT_AEST
                                                                 ELSE E.QUICK_REGISTRATION_AEST
                                                            END, GETDATE())) / 365.25)

     -- Output:

          ,FLOOR(DATEDIFF(DAY, E.PRESENT_DOB, COALESCE(CASE
                                                                 WHEN TDT.TRIAGED_AT_AEST IS NOT NULL
                                                                      AND E.QUICK_REGISTRATION_AEST IS NULL
                                                                 THEN TDT.TRIAGED_AT_AEST
                                                                 WHEN TDT.TRIAGED_AT_AEST < E.QUICK_REGISTRATION_AEST
                                                                 THEN TDT.TRIAGED_AT_AEST
                                                                 ELSE E.QUICK_REGISTRATION_AEST
                                                            END, current_timestamp()) AS PRESENT_AGE) / 365.25)
```
3. SEX_CODE placed in the wrong position:

```sql
     -- Input:

          ,CASE 
                                   WHEN LEFT(E.[PRESENT_GENDER],1) = 'M' THEN 1
                                   WHEN LEFT(E.[PRESENT_GENDER],1) = 'F' THEN 2
                                   WHEN LEFT(E.[PRESENT_GENDER],1) = 'I' THEN 3
                                   ELSE 9
                              END AS SEX_CODE

     -- Output:
          ,CASE 
                                   WHEN LEFT(E.`PRESENT_GEND AS SEX_CODEER`,1) = 'M' THEN 1
                                   WHEN LEFT(E.`PRESENT_GENDER`,1) = 'F' THEN 2
                                   WHEN LEFT(E.`PRESENT_GENDER`,1) = 'I' THEN 3
                                   ELSE 9
                              END
     ```