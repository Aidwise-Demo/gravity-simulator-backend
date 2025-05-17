import pandas as pd
def status_calculation(df):
    numeric_cols = ['Predicted_Actual_Value', 'current*',
                    'Predicted_Industry_average', 'Cut_off_value',"target_value"]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

    # Compute gaps using the dynamic df["target_value"]
    df['gap_curr'] = ((df["target_value"] - df['Predicted_Actual_Value']) / df['Cut_off_value']).round(4)
    df['gap_expected'] = ((df["target_value"] - df['current*']) / df['Cut_off_value']).round(4)
    df['gap_industry'] = (
                (df['Predicted_Industry_average'] - df['Predicted_Actual_Value']) / df['Cut_off_value']).round(4)

    # Compute composite risk score
    df['risk_composite'] = (
            0.5 * df['gap_curr'] +
            0.3 * df['gap_expected'] +
            0.2 * df['gap_industry']
    ).round(4)

    # Risk level categorization
    def get_risk_level(score):
        if score <= 0.1:
            return 'Low'
        elif score <= 0.25:
            return 'Medium'
        else:
            return 'High'

    df['expected_risk'] = df['risk_composite'].apply(get_risk_level)

    # Select columns for output
    # output = df[['Business_Vertical', 'Metrics', 'expected_risk']]
    return df