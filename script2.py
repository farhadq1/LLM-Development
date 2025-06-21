import pandas as pd
import numpy as np # For handling BLANK() equivalents

# --- 1. Sample DataFrames (no ID fields in this version) ---

# Derisking table (main table to iterate over)
derisking_data = {
    'Name': ['Cust', 'OrderDate', 'amount', 'region', 'Description', 'ID', 'Sales', 'prod_id', 'Name']
}
derisking_df = pd.DataFrame(derisking_data)

# Target Df table (contains ColumnName and BusinessName, no explicit link to Derisking)
target_data = {
    'ColumnName': ['Customer ID', 'Order Date', 'Sales Amount', 'Product Details', 'Product_Identifier', 'Employee Staffing', 'Staff Name'],
    'BusinessName': ['Customer Data', 'Sales Metrics', 'Geo Analytics', 'Product Catalog', 'Inventory Items', 'HR Management', 'Employee Relations']
}
target_df = pd.DataFrame(target_data)

print("--- Original DataFrames (No IDs) ---")
print("Derisking_df:\n", derisking_df)
print("\nTarget_df:\n", target_df)
print("-" * 30)

# --- 2. Define Global Search Strings from Target_df (Equivalent to DAX VARs for global strings) ---
# This assumes you want to search against a concatenated string of ALL ColumnNames/BusinessNames

# Join all ColumnName values from Target_df into one string, space-separated, and lowercase
all_target_column_names_lower = " ".join(target_df['ColumnName'].str.lower().tolist())
# Join all BusinessName values from Target_df into one string, space-separated, and lowercase
all_target_business_names_lower = " ".join(target_df['BusinessName'].str.lower().tolist())

# Add extra spaces at start/end for robust "word" searching later, similar to DAX prep
all_target_column_names_lower_padded = f" {all_target_column_names_lower} "
all_target_business_names_lower_padded = f" {all_target_business_names_lower} "

print(f"--- Global Search Strings ---")
print(f"ColumnNameLower (concatenated): '{all_target_column_names_lower}'")
print(f"BusinessNameLower (concatenated): '{all_target_business_names_lower}'")
print("-" * 30)

# --- 3. Implement the DAX Logic ---

# --- Var matchtable ---
# 1. Filter: Not (lower('Derisking' [Name]) = "description")
matchtable = derisking_df[derisking_df['Name'].str.lower() != "description"].copy()

# 2. Add 'keywordlower', 'matchlength'
matchtable['keywordlower'] = matchtable['Name'].str.lower()
matchtable['matchlength'] = matchtable['Name'].str.len()

# 3. Add 'matchincolumn', 'matchinbusiness'
# Now we compare each Derisking Name against the global concatenated strings
def calculate_match_status_global(keyword_full_name, keyword_lower, global_target_text_lower_padded, global_target_text_lower_unpadded):
    if len(keyword_full_name) <= 3:
        # Check for delimited matches for short names
        # " keyword ", "_keyword ", "_keyword_"
        condition1 = f" {keyword_lower} " in global_target_text_lower_padded
        condition2 = f"_{keyword_lower} " in global_target_text_lower_unpadded
        condition3 = f"_{keyword_lower}_" in global_target_text_lower_unpadded
        return condition1 or condition2 or condition3
    else:
        # Simple substring search for longer names
        return keyword_lower in global_target_text_lower_unpadded

# Apply the global comparison for each row in matchtable
matchtable['matchincolumn'] = matchtable.apply(
    lambda row: calculate_match_status_global(
        row['Name'],
        row['keywordlower'],
        all_target_column_names_lower_padded,
        all_target_column_names_lower
    ),
    axis=1
)

matchtable['matchinbusiness'] = matchtable.apply(
    lambda row: calculate_match_status_global(
        row['Name'],
        row['keywordlower'],
        all_target_business_names_lower_padded,
        all_target_business_names_lower
    ),
    axis=1
)

print("\n--- matchtable Output (with Global Comparison) ---")
print(matchtable)
print("-" * 30)

# --- var validpartialmatch ---
validpartialmatch = matchtable[
    (matchtable['matchincolumn'] == True) | (matchtable['matchinbusiness'] == True)
].copy()

print("\n--- validpartialmatch Output ---")
print(validpartialmatch)
print("-" * 30)

# --- var toppartialmatch ---
# Summarize by 'Derisking'[Name] and add 'matchlength'
# Group by 'Name' and pick the first (which will be the only one for unique Names)
summarized_matches = validpartialmatch[['Name', 'matchlength']].drop_duplicates(subset=['Name']).copy()

# Sort by matchlength descending and pick the top 1
toppartialmatch = summarized_matches.sort_values(by='matchlength', ascending=False).head(1).copy()

print("\n--- toppartialmatch Output ---")
print(toppartialmatch)
print("-" * 30)

# --- var partialkeyword ---
# Select 'Derisking'[Name] and rename it to 'keyword'
if not toppartialmatch.empty:
    partialkeyword = toppartialmatch[['Name']].rename(columns={'Name': 'keyword'})
else:
    partialkeyword = pd.DataFrame(columns=['keyword']) # Empty DataFrame if no match

print("\n--- partialkeyword Output ---")
print(partialkeyword)
print("-" * 30)

# --- var matchkeyword ---
# if(countrows(Partialkeyword) > 0, firstnonblank(partialkeyword,1), Blank())
if not partialkeyword.empty:
    matchkeyword = partialkeyword['keyword'].iloc[0]
else:
    matchkeyword = np.nan # Using numpy.nan to represent BLANK()

print(f"\n--- matchkeyword Output: {matchkeyword} ---")
print("-" * 30)

# --- var triggerpartialmatch ---
# switch(true(), lower(matchkeyword) = "name" && (containstring(columnName,'employee') || (containstring(columnName,'staff')) ), 'Partialmatch: Employee')

triggerpartialmatch = np.nan # Default to BLANK() (NaN)

if pd.notna(matchkeyword) and (matchkeyword.lower() == "name"):
    # Here, 'columnName' in the DAX would refer to the *global* all_target_column_names_lower
    # for the CONTAINSSTRING check.
    # The DAX CONTAINSTRING is case-insensitive, so we ensure the target string is lowercased.
    if ("employee" in all_target_column_names_lower) or ("staff" in all_target_column_names_lower):
        triggerpartialmatch = 'Partialmatch: Employee'

print(f"\n--- triggerpartialmatch Output: {triggerpartialmatch} ---")
print("-" * 30)

# --- Final Return Logic ---
# Return (if not isblank(triggeredpartialmatch), triggeredpartialmatch,
#          (if countrows(partialkeyword) > 0, "partial manual review required", BLANK()))

final_result = np.nan

if pd.notna(triggerpartialmatch): # not isblank(triggeredpartialmatch)
    final_result = triggerpartialmatch
else: # triggeredpartialmatch is BLANK
    if not partialkeyword.empty: # countrows(partialkeyword) > 0
        final_result = "partial manual review required"
    else: # partialkeyword is empty
        final_result = np.nan # BLANK()

print(f"\n--- Final Result Output: {final_result} ---")
print("-" * 30)
