import pandas as pd
import numpy as np # For handling BLANK() equivalents

# --- 1. Sample DataFrames ---

# Derisking table (used for lookup names)
derisking_data = {
    'Name': ['Cust', 'OrderDate', 'amount', 'region', 'Description', 'ID', 'Sales', 'prod_id', 'Name', 'Employee_ID', 'Project Name'],
    # Added 'Project Name' to 'derisking' for a potential columnmatchfound example
    # Assuming 'derisking' might also have a 'columnsname' for the 'columnmatchfound' original DAX
    # BUT based on your latest DAX: Filter('derisking',lower('derisking'[name]) = ColumnNameLower)
    # We will use 'derisking'[name] for columnmatchfound's filter.
}
derisking_df = pd.DataFrame(derisking_data)

# Target Df table (where new columns will be added)
target_data = {
    'columnsname': ['Customer ID', 'Order Date Field', 'Sales Amount', 'Project Name', 'Product_Identifier', 'Employee Staffing', 'Staff Name', 'Office Location', 'Cust'],
    'BusinessName': ['Customer Data', 'Sales Metrics', 'Geo Analytics', 'Project Management', 'Inventory Items', 'HR Management', 'Employee Relations', 'Facility Management', 'Customer Data'],
    'TargetID': range(1, 10) # Just for display/identifying rows
}
target_df = pd.DataFrame(target_data)

print("--- Original DataFrames ---")
print("Derisking_df:\n", derisking_df)
print("\nTarget_df:\n", target_df)
print("-" * 30)

# --- 2. Pre-filter Derisking Names (for efficiency, as this filter is constant) ---
filtered_derisking_names_data = derisking_df[
    derisking_df['Name'].str.lower() != "description"
].copy()
filtered_derisking_names_data['keywordlower'] = filtered_derisking_names_data['Name'].str.lower()
filtered_derisking_names_data['matchlength'] = filtered_derisking_names_data['Name'].str.len()

print("--- Filtered Derisking Data for Matching ---")
print(filtered_derisking_names_data)
print("-" * 30)

# --- 3. Define the Core Matching Logic (for a single Derisking Name against a single Target string) ---
# This is equivalent to the DAX SEARCH function's logic
def check_single_match(derisking_name_full, derisking_name_lower, target_text_original):
    """
    Checks if a single derisking name matches a single target string based on DAX SEARCH rules.
    Returns True if matched, False otherwise.
    """
    if pd.isna(target_text_original) or not isinstance(target_text_original, str):
        return False # DAX SEARCH on BLANK or non-string would result in 0 or error, handled as no match

    target_text_lower = target_text_original.lower()

    is_short_derisking_name = len(derisking_name_full) <= 3

    if is_short_derisking_name:
        # Delimited search for short names (like " ID ", "_ID ", "_ID_")
        target_text_lower_padded = f" {target_text_lower} " # Pad target for word boundary at start/end
        if (f" {derisking_name_lower} " in target_text_lower_padded) or \
           (f"_{derisking_name_lower} " in target_text_lower) or \
           (f"_{derisking_name_lower}_" in target_text_lower):
            return True
    else:
        # Substring search for longer names
        if derisking_name_lower in target_text_lower:
            return True
    return False

# --- 4. Main Function to process each row of target_df, mimicking the DAX Calculated Column ---
def process_full_dax_for_target_row(target_row, filtered_derisking_names_data, all_derisking_names_lower_set):
    # DAX VARs for current row context (these change for each target_df row)
    current_column_name_original = target_row['columnsname']
    current_business_name_original = target_row['BusinessName']
    
    # var ColumnNameLower = lower('target_df'[columnsname])
    # Handle potential non-string/NaN values by converting to string and then lowercasing
    ColumnNameLower = str(current_column_name_original).lower() if pd.notna(current_column_name_original) else ""
    
    # var BusinessNameLower = lower('target_df'[BusinessName])
    BusinessNameLower = str(current_business_name_original).lower() if pd.notna(current_business_name_original) else ""

    # --- var columnmatchfound ---
    # calculate(firstnonblank('derisking'[name],1), Filter('derisking',lower('derisking'[name]) = ColumnNameLower))
    # This checks if the current target_df row's ColumnNameLower exactly matches any lowercased Derisking.Name
    col_match_found_val = np.nan
    if ColumnNameLower in all_derisking_names_lower_set:
        # Find the original casing of the matched Derisking name for firstnonblank
        # This is a simplification; in DAX, FIRSTNONBLANK would get the actual 'derisking'[name]
        # We can find one example of it from our filtered_derisking_names_data
        matched_derisking_row = filtered_derisking_names_data[
            filtered_derisking_names_data['keywordlower'] == ColumnNameLower
        ]
        if not matched_derisking_row.empty:
            col_match_found_val = matched_derisking_row['Name'].iloc[0]
    
    # --- Var matchtable ---
    # This involves iterating through 'Derisking' for each 'target_df' row's column/business name
    
    # Collect all valid Derisking matches for THIS target_row
    all_valid_derisking_matches_for_this_row = []

    for idx, derisking_row in filtered_derisking_names_data.iterrows():
        derisking_name = derisking_row['Name']
        derisking_name_lower = derisking_row['keywordlower']
        derisking_match_length = derisking_row['matchlength']

        # 'matchincolumn' logic for this Derisking Name against THIS Target Df row's ColumnName
        match_in_col = check_single_match(derisking_name, derisking_name_lower, current_column_name_original)
        if match_in_col:
            # Storing Derisking Name, Length, and Type of Target Column it matched ('Column' or 'Business')
            all_valid_derisking_matches_for_this_row.append((derisking_name, derisking_match_length, 'Column'))

        # 'matchinbusiness' logic for this Derisking Name against THIS Target Df row's BusinessName
        match_in_bus = check_single_match(derisking_name, derisking_name_lower, current_business_name_original)
        if match_in_bus:
            all_valid_derisking_matches_for_this_row.append((derisking_name, derisking_match_length, 'Business'))

    # --- var validpartialmatch (for this target_row's context) ---
    # True if ANY Derisking name matched THIS target_row's Column OR Business Name
    is_valid_partial_match = bool(all_valid_derisking_matches_for_this_row) # True if list is not empty

    # --- var toppartialmatch (for this target_row's context) ---
    # Find the best Derisking match (longest Name) among those that matched THIS target_row
    matched_derisking_keyword_from_topn = np.nan # Derisking Name (keyword)
    matched_derisking_length_from_topn = np.nan # Length of that Derisking Name
    matched_type_from_topn = np.nan # Which type of target field it matched ('Column' or 'Business')

    if is_valid_partial_match:
        # Sort by length descending, then by name (arbitrary tie-breaker for consistent "first" match)
        best_match_info = sorted(all_valid_derisking_matches_for_this_row, key=lambda x: (x[1], x[0]), reverse=True)[0]
        matched_derisking_keyword_from_topn = best_match_info[0]
        matched_derisking_length_from_topn = best_match_info[1]
        matched_type_from_topn = best_match_info[2] # Store which type of target field it matched

    # --- var partialkeyword ---
    # This refers to the keyword (Derisking Name) from toppartialmatch.
    # In DAX, 'partialkeyword' is often a table (e.g., SELECTCOLUMNS(toppartialmatch, "keyword", 'Derisking'[Name]))
    # For a scalar result, we extract it here.
    partialkeyword_scalar = matched_derisking_keyword_from_topn

    # --- var matchkeyword ---
    # if(countrows(Partialkeyword) > 0,firstnonblank(partialkeyword,1),Blank())
    # This is equivalent to our partialkeyword_scalar if it's not NaN
    matchkeyword_val = partialkeyword_scalar

    # --- var matchlength ---
    # if(countrows(toppartialmatch) > 0,firstnonblank(toppartialmatch,[matchlength]),Blank())
    # This is equivalent to our matched_derisking_length_from_topn if it's not NaN
    matchlength_val = matched_derisking_length_from_topn

    # --- var triggerpartialmatch ---
    # switch(true(),lower(matchkeyword) = "name" && (containstring(columnName,'employee') || (containstring(columnName,'staff') ), 'Parialmatch: Employee')
    # `columnName` here logically refers to the `current_column_name_original` of THIS target_df row.
    triggerpartialmatch_val = np.nan
    if pd.notna(matchkeyword_val) and (str(matchkeyword_val).lower() == "name"):
        current_col_name_lower_for_trigger = str(current_column_name_original).lower() if pd.notna(current_column_name_original) else ""
        if ("employee" in current_col_name_lower_for_trigger) or ("staff" in current_col_name_lower_for_trigger):
            triggerpartialmatch_val = 'Partialmatch: Employee'

    # --- FINAL RETURN Logic ---
    # if (is not blank(columnmatchfound),columnNameLower),
    # if not isblank(triggeredpartialmatch),triggeredpartialmatch,(if countrows(partialkeyword) > 0 "partial manual review required"
    final_result_for_row = np.nan

    if pd.notna(col_match_found_val): # if columnmatchfound is not BLANK
        final_result_for_row = ColumnNameLower # Return the lowercased original target_df column name
    elif pd.notna(triggerpartialmatch_val): # else if triggerpartialmatch is not BLANK
        final_result_for_row = triggerpartialmatch_val
    elif pd.notna(partialkeyword_scalar): # else if partialkeyword (the actual best match name) is not BLANK
        final_result_for_row = "partial manual review required"
    # else: final_result_for_row remains np.nan (BLANK())

    # Return all calculated values for the new columns
    return pd.Series({
        'ColumnNameLower_Var': ColumnNameLower,
        'BusinessNameLower_Var': BusinessNameLower,
        'ColumnMatchFound_DeriskingName': col_match_found_val, # The actual Derisking name if exact match found
        'MatchedDeriskingKeyword': matched_derisking_keyword_from_topn, # Best Derisking match from partial logic
        'MatchedKeywordLength_Derisking': matched_derisking_length_from_topn,
        'MatchedKeywordType_TargetColumn': matched_type_from_topn, # 'Column' or 'Business' that was matched
        'TriggeredPartialMatch_Result': triggerpartialmatch_val,
        'FinalDeriskingStatus': final_result_for_row
    })

# --- Prepare data for the apply function ---
# For columnmatchfound, we need a quick way to check if ColumnNameLower exists in derisking.name
all_derisking_names_lower_set = set(filtered_derisking_names_data['keywordlower'])

# --- 5. Apply the processing function to each row of Target Df ---
print("\n--- Applying DAX logic row by row to Target DataFrame ---")
new_columns_df = target_df.apply(
    lambda row: process_full_dax_for_target_row(row, filtered_derisking_names_data, all_derisking_names_lower_set),
    axis=1
)

# --- 6. Combine original target_df with the new calculated columns ---
final_target_df_with_results = pd.concat([target_df, new_columns_df], axis=1)

print("\n--- Final Target DataFrame with All Calculated Columns Per Row ---")
print(final_target_df_with_results)
print("-" * 30)
