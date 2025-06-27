import pandas as pd
import numpy as np

def get_dtr_element_by_combination_with_fallback(df):
    """
    This function processes a DataFrame to pick the appropriate 'element name'
    for each unique combination of 'field name' and 'business name'.
    It prioritizes non-blank and non-'No DTR' element names.
    If no such valid element is found for a combination, it falls back to 'No DTR'.

    Args:
        df (pd.DataFrame): The input DataFrame with columns
                           'field name', 'business name', and 'element name'.

    Returns:
        pd.DataFrame: The DataFrame with an additional column 'selected_dtr_element'
                      containing the chosen element name for each combination.
                      It will be 'No DTR' if no valid element is found.
    """
    # Create a new column to store the selected DTR element, initialized to NaN
    df['selected_dtr_element'] = np.nan

    # Define the logic for selecting an element within each group
    def select_element_for_group(group):
        # 1. Try to find an element that is NOT blank and NOT 'No DTR'
        valid_elements_in_group = group[
            group['element name'].notna() & (group['element name'] != 'No DTR')
        ]['element name']

        if not valid_elements_in_group.empty:
            # If valid elements exist, pick the first one
            return valid_elements_in_group.iloc[0]
        else:
            # 2. If no valid elements are found, fall back to 'No DTR'
            return 'No DTR'

    # Apply the selection logic to each group formed by 'field name' and 'business name'
    # The result of apply will be a Series aligned with the original DataFrame's index,
    # which allows direct assignment.
    df['selected_dtr_element'] = df.groupby(['field name', 'business name'], group_keys=False).apply(select_element_for_group)

    return df

if __name__ == '__main__':
    # Sample DataFrame for demonstration based on your clarification
    data = {
        'field name': ['change type', 'ETL', 'load date', 'Load Date', 'ETL', 'ytd cap', 'year to date cap', 'zldo', 'Zero Trailer Rate Indicator', 'A', 'A', 'B', 'B', 'C', 'C', 'D', 'D'],
        'business name': ['ETL', 'change type', 'Load Date', 'ETL', 'load date', 'year to date cap', 'ytd cap', 'Zero Trailer Rate Indicator', 'zldo', 'X', 'X', 'Y', 'Y', 'Z', 'Z', 'W', 'W'],
        'element name': ['change type', 'No DTR', 'load date', 'ETL', 'ETL', 'No DTR', 'No DTR', 'No DTR', 'No DTR', 'DTR1', 'No DTR', 'No DTR', 'DTR2', '', 'DTR3', np.nan, 'No DTR']
    }
    df = pd.DataFrame(data)

    print("Original DataFrame:")
    print(df)
    print("\n" + "="*50 + "\n")

    # Apply the function
    df_processed = get_dtr_element_by_combination_with_fallback(df.copy()) # Use .copy() to avoid modifying the original DataFrame

    print("Processed DataFrame:")
    print(df_processed)

    print("\n" + "="*50 + "\n")
    print("Explanation of specific rows in the processed output:")
    print("- Rows for ('change type', 'ETL') and ('load date', 'Load Date') pick the specific DTR because they are valid.")
    print("- Rows for ('ETL', 'change type'), ('ytd cap', 'year to date cap'), ('zldo', 'Zero Trailer Rate Indicator'), etc., all resolve to 'No DTR' because for their respective combinations, no valid DTR (i.e., not blank and not 'No DTR') was found. For example, the ('ETL', 'change type') combination only has 'No DTR', so 'No DTR' is picked.")
    print("- For ('A', 'X'): 'DTR1' is picked because it's a valid DTR. 'No DTR' is ignored because a valid one exists.")
    print("- For ('B', 'Y'): 'DTR2' is picked because it's a valid DTR.")
    print("- For ('C', 'Z'): 'DTR3' is picked because it's a valid DTR. The blank element '' is ignored.")
    print("- For ('D', 'W'): Both elements are NaN and 'No DTR'. Since no valid DTR exists, 'No DTR' is picked as the fallback.")
