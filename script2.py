import pandas as pd
import numpy as np

# --- 1. Sample DataFrames (Modified target_df with 'Target_Category') ---
target_data = {
    'columnsname': ['Customer ID', 'Order Date Field', 'Sales Amount', 'Project Name', 'Product_Identifier', 'Employee Staffing', 'Staff Name', 'Office Location', 'Cust', 'Customer Account', 'Account Number', 'Paramount Pictures', 'Sales'],
    'BusinessName': ['Customer Data', 'Sales Metrics', 'Geo Analytics', 'Project Management', 'Inventory Items', 'HR Management', 'Employee Relations', 'Facility Management', 'Customer Data', 'Finance Data', 'Financial Records', 'Film Studio', 'Sales'],
    'TargetID': range(1, 14),
    # --- NEW: Assuming a 'Target_Category' column in target_df ---
    'Target_Category': [
        'Customer_Info', 'Financial_Info', 'Financial_Info', 'Project_Info', 'Product_Info',
        'HR_Info', 'HR_Info', 'Geographic_Info', 'Customer_Info', 'Financial_Info',
        'Financial_Info', 'General_Info', 'Financial_Info'
    ]
}
target_df = pd.DataFrame(target_data)

# --- Rulebook DataFrame ---
rulebook_data = {
    'Data Category': ['Financial_Info', 'Customer_Info', 'HR_Info', 'Product_Info', 'Geographic_Info', 'Financial_Info', 'Project_Info'],
    'Rulebook Element': ['Amount', 'Customer', 'Employee', 'ID', 'Region', 'Account', 'Project Name']
}
rulebook_df = pd.DataFrame(rulebook_data)


print("--- Original DataFrames ---")
print("\nTarget_df:\n", target_df)
print("\nRulebook_df:\n", rulebook_df)
print("-" * 30)

# --- 2. Pre-process Rulebook for efficiency ---
rulebook_df['data_category_lower'] = rulebook_df['Data Category'].str.lower()
rulebook_df['rulebook_element_lower'] = rulebook_df['Rulebook Element'].str.lower()
rulebook_df['rulebook_element_length'] = rulebook_df['Rulebook Element'].str.len()


print("\n--- Pre-processed Rulebook Data ---")
print(rulebook_df)
print("-" * 30)


# --- 3. Modified process_rulebook_match_for_row function ---
def process_rulebook_match_for_row(target_row, rulebook_data_processed):
    # Get target category, converted to lower for case-insensitive matching
    target_category_lower = str(target_row['Target_Category']).lower() \
                            if pd.notna(target_row['Target_Category']) else ""

    current_column_name_original = target_row['columnsname']
    current_business_name_original = target_row['BusinessName']
    
    ColumnNameLower = str(current_column_name_original).lower() if pd.notna(current_column_name_original) else ""
    BusinessNameLower = str(current_business_name_original).lower() if pd.notna(current_business_name_original) else ""

    final_matched_rulebook_element = np.nan # Initialize output to NaN
    
    # List to store all rulebook elements that match for the current target row
    # Stores (Rulebook Element full string, its length)
    possible_rulebook_matches = [] 

    for idx, rule_row in rulebook_data_processed.iterrows():
        rule_category_lower = rule_row['data_category_lower'] # Rulebook's category (lowercased)
        rule_element_lower = rule_row['rulebook_element_lower'] # Rulebook's element (lowercased)
        rule_element_full = rule_row['Rulebook Element'] # Original casing for output
        rule_element_length = rule_row['rulebook_element_length']

        # --- First Check: Does the target's category match the rulebook's Data Category? ---
        if target_category_lower == rule_category_lower:
            # --- Only if categories match, proceed to check for string containment ---
            if (rule_element_lower in ColumnNameLower) or \
               (rule_element_lower in BusinessNameLower):
                possible_rulebook_matches.append((rule_element_full, rule_element_length))
    
    # If any rulebook elements matched based on both conditions, select the "best" one
    if possible_rulebook_matches:
        # Sort by element length (descending) then by element name (descending) for ties
        best_rulebook_match_info = sorted(
            possible_rulebook_matches,
            key=lambda x: (x[1], x[0]), 
            reverse=True 
        )[0] # Get the first element (the best match) from the sorted list
        
        final_matched_rulebook_element = best_rulebook_match_info[0] # The matched Rulebook Element

    # Return only the matched rulebook element
    return pd.Series({
        'Matched_Rulebook_Element': final_matched_rulebook_element
    })

# --- 4. Apply the logic to each row of the target DataFrame ---
print("\n--- Applying Rulebook matching logic to Target DataFrame (with category check) ---")
new_column_results = target_df.apply(
    lambda row: process_rulebook_match_for_row(row, rulebook_df), 
    axis=1
)

# --- 5. Concatenate the new columns back to the original target DataFrame ---
final_target_df_with_matched_rules = pd.concat([target_df, new_column_results], axis=1)

print("\n--- Final Target DataFrame with Matched Rulebook Element ---")
print(final_target_df_with_matched_rules)
print("-" * 30)
