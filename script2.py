import pandas as pd
import numpy as np

# --- 1. Define the check_single_match function (as provided in your last prompt) ---
def check_single_match(derisking_name_full, derisking_name_lower, target_text_original):
    """
    Checks if a single derisking name matches a single target string based on DAX SEARCH rules.
    Returns True if matched, False otherwise.
    """
    if pd.isna(target_text_original) or not isinstance(target_text_original, str):
        return False

    target_text_lower = target_text_original.lower()

    is_short_derisking_name = len(derisking_name_full) <= 3

    if is_short_derisking_name:
        target_text_lower_padded = f" {target_text_lower} "
        if (f" {derisking_name_lower} " in target_text_lower_padded) or \
           (f"_{derisking_name_lower} " in target_text_lower) or \
           (f"_{derisking_name_lower}_" in target_text_lower):
            return True
    else:
        if derisking_name_lower in target_text_lower:
            return True
    return False

# --- 2. Sample DataFrames (as before) ---
derisking_data = {
    'Name': ['Cust', 'OrderDate', 'amount', 'region', 'ID', 'Sales', 'prod_id', 'Name', 'Employee_ID', 'Project Name', 'Account No', 'DAS Internal Account Number', 'Description']
}
derisking_df = pd.DataFrame(derisking_data)

target_data = {
    'columnsname': ['Customer ID', 'Order Date Field', 'Sales Amount', 'Project Name', 'Product_Identifier', 'Employee Staffing', 'Staff Name', 'Office Location', 'Cust', 'Customer Account', 'Account Number', 'Paramount Pictures', 'Sales'], # Added 'Sales' to BusinessName to test exact match
    'BusinessName': ['Customer Data', 'Sales Metrics', 'Geo Analytics', 'Project Management', 'Inventory Items', 'HR Management', 'Employee Relations', 'Facility Management', 'Customer Data', 'Finance Data', 'Financial Records', 'Film Studio', 'Sales'], # Added 'Sales' here
    'TargetID': range(1, 14) # Adjusted range
}
target_df = pd.DataFrame(target_data)

print("--- Original DataFrames ---")
print("Derisking_df:\n", derisking_df)
print("\nTarget_df:\n", target_df)
print("-" * 30)

# --- 3. Pre-process Derisking Names for efficiency ---
filtered_derisking_names_data = derisking_df[
    derisking_df['Name'].str.lower() != "description"
].copy()
filtered_derisking_names_data['keywordlower'] = filtered_derisking_names_data['Name'].str.lower()
filtered_derisking_names_data['matchlength'] = filtered_derisking_names_data['Name'].str.len()

all_derisking_names_lower_set = set(filtered_derisking_names_data['keywordlower'])

print("--- Filtered Derisking Data for Matching (Pre-processed) ---")
print(filtered_derisking_names_data)
print("-" * 30)

# --- 4. MODIFIED process_dax_match_logic_for_row function ---
def process_dax_match_logic_for_row(target_row, derisking_data_for_match, derisking_lower_set):
    current_column_name_original = target_row['columnsname']
    current_business_name_original = target_row['BusinessName']
    
    ColumnNameLower = str(current_column_name_original).lower() if pd.notna(current_column_name_original) else ""
    BusinessNameLower = str(current_business_name_original).lower() if pd.notna(current_business_name_original) else ""

    # --- ENHANCED EXACT MATCH LOGIC ---
    # Combine checks for exact match on both ColumnNameLower and BusinessNameLower
    exact_match_derisking_name = np.nan 

    # Check for exact match with ColumnNameLower
    if ColumnNameLower in derisking_lower_set:
        matched_derisking_row = derisking_data_for_match[
            derisking_data_for_match['keywordlower'] == ColumnNameLower
        ]
        if not matched_derisking_row.empty:
            exact_match_derisking_name = matched_derisking_row['Name'].iloc[0]
            # If an exact match is found here, we can prioritize it immediately.
            # No need to check BusinessNameLower if ColumnNameLower already provided a top match.
    
    # If no exact match on ColumnNameLower, check for exact match on BusinessNameLower
    if pd.isna(exact_match_derisking_name) and BusinessNameLower in derisking_lower_set:
        matched_derisking_row = derisking_data_for_match[
            derisking_data_for_match['keywordlower'] == BusinessNameLower
        ]
        if not matched_derisking_row.empty:
            exact_match_derisking_name = matched_derisking_row['Name'].iloc[0]


    # --- Remaining logic for partial matches (unchanged) ---
    all_valid_derisking_matches_for_this_row = []

    for idx, derisking_row in derisking_data_for_match.iterrows():
        derisking_name = derisking_row['Name']
        derisking_name_lower = derisking_row['keywordlower']
        derisking_match_length = derisking_row['matchlength']

        match_in_col = check_single_match(derisking_name, derisking_name_lower, current_column_name_original)
        if match_in_col:
            all_valid_derisking_matches_for_this_row.append((derisking_name, derisking_match_length, 'Column'))

        match_in_bus = check_single_match(derisking_name, derisking_name_lower, current_business_name_original)
        if match_in_bus:
            all_valid_derisking_matches_for_this_row.append((derisking_name, derisking_match_length, 'Business'))
    
    matched_derisking_keyword_from_partial = np.nan 
    if all_valid_derisking_matches_for_this_row:
        best_match_info = sorted(all_valid_derisking_matches_for_this_row, 
                                 key=lambda x: (x[1], x[0]), reverse=True)[0]
        matched_derisking_keyword_from_partial = best_match_info[0]

    triggered_partial_match_result = np.nan
    if pd.notna(matched_derisking_keyword_from_partial) and str(matched_derisking_keyword_from_partial).lower() == "name":
        if ("employee" in ColumnNameLower) or ("staff" in ColumnNameLower):
            triggered_partial_match_result = 'Partialmatch: Employee'

    # --- Final result based on precedence ---
    final_matched_derisking_name = np.nan

    if pd.notna(exact_match_derisking_name): # Now uses the combined exact match
        final_matched_derisking_name = exact_match_derisking_name
    elif pd.notna(triggered_partial_match_result):
        final_matched_derisking_name = triggered_partial_match_result
    elif pd.notna(matched_derisking_keyword_from_partial):
        final_matched_derisking_name = matched_derisking_keyword_from_partial

    return pd.Series({
        'Matched_Derisking_Name': final_matched_derisking_name
    })

# --- 5. Apply the logic to each row of the target DataFrame ---
print("\n--- Applying DAX-like matching logic to Target DataFrame (with ENHANCED Exact Match) ---")
new_column_results = target_df.apply(
    lambda row: process_dax_match_logic_for_row(row, filtered_derisking_names_data, all_derisking_names_lower_set),
    axis=1
)

# --- 6. Concatenate the new column back to the original target DataFrame ---
final_target_df_with_matched_names = pd.concat([target_df, new_column_results], axis=1)

print("\n--- Final Target DataFrame with Matched Derisking Names ---")
print(final_target_df_with_matched_names)
print("-" * 30)
