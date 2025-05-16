import pandas as pd
from sklearn.linear_model import LinearRegression
import numpy as np


def generate_projection_output(
    df_simulator, 
    df_initiatives, 
    df_industry_avg, 
    output_path="projection_output.xlsx"
):

    # Standardize column names for business vertical
    def standardize_bu(df):
        if 'BUName' in df.columns:
            df = df.rename(columns={'BUName': 'Business_Vertical'})
        elif 'Bsuiness_vrtical' in df.columns:
            df = df.rename(columns={'Bsuiness_vrtical': 'Business_Vertical'})
        return df

    df_simulator = standardize_bu(df_simulator)
    df_initiatives = standardize_bu(df_initiatives)
    df_industry_avg = standardize_bu(df_industry_avg)

    # Prepare df2
    year = pd.Timestamp.now().year
    df2 = df_simulator[['Metrics', 'Year', 'Actual_Value', 'Business_Vertical']].drop_duplicates()
    df2 = df2[df2["Year"] <= year]
    df2 = df2[df2['Actual_Value'].notna()]

    # Projection function
    def values_projection(df2):
        projections = []
        for (vertical, metric), group in df2.groupby(['Business_Vertical', 'Metrics']):
            group = group.sort_values('Year')
            X = group['Year'].values.reshape(-1, 1)
            y = group['Actual_Value'].values
            if len(X) > 1:
                model = LinearRegression()
                model.fit(X, y)
                next_year = group['Year'].max() + 1
                predicted = model.predict(np.array([[next_year]]))[0]
                projections.append({
                    'Business_Vertical': vertical,
                    'Metrics': metric,
                    'Year': next_year,
                    'Predicted_Actual_Value': predicted
                })
        return pd.DataFrame(projections)

    projections_df = values_projection(df2)

    # Initiatives score function
    def initiatives_scores(inittives):
        year = pd.Timestamp.now().year
        inittives_filtered = inittives[inittives['End Year'] >= year]
        initiaves_factor = inittives_filtered.groupby('Business_Vertical').agg(
            on_track_count=('Status', lambda x: (x == 'On Track').sum()),
            total_initiatives=('Initiative', 'count')
        ).reset_index()
        initiaves_factor["factor"] = initiaves_factor["on_track_count"] / initiaves_factor["total_initiatives"]
        number_od_days_from_today = pd.Timestamp.now().dayofyear
        days_left = (365 - number_od_days_from_today) / 365
        initiaves_factor["factor_based_on_time_left"] = days_left
        initiaves_factor["total_initaitive_score"] = initiaves_factor["total_initiatives"] / (
            max(initiaves_factor["total_initiatives"])
        )
        initiaves_factor["final_factor"] = 1 + (0.25 + initiaves_factor["factor"]) * initiaves_factor["factor_based_on_time_left"] * (
            0.25 + initiaves_factor["total_initaitive_score"]
        )
        return initiaves_factor[["Business_Vertical", "final_factor"]]

    initiaves_factor = initiatives_scores(df_initiatives)

    # Default final_factor if missing
    number_od_days_from_today = pd.Timestamp.now().dayofyear
    days_left = (365 - number_od_days_from_today) / 365
    final_factor = 1 + (0.25 * days_left * 0.25)

    # Merge projections and initiatives
    projections_df_merged = pd.merge(
        projections_df, initiaves_factor, on="Business_Vertical", how="left"
    )
    projections_df_merged["final_factor"] = projections_df_merged["final_factor"].fillna(final_factor)
    projections_df_merged["current*"] = projections_df_merged["Predicted_Actual_Value"] * projections_df_merged["final_factor"]

    # Industry average projection
    industry_average = df_industry_avg.copy()
    industry_average = standardize_bu(industry_average)
    projections_df_industry_avg = values_projection(industry_average)
    projections_df_industry_avg = projections_df_industry_avg.rename(
        columns={"Predicted_Actual_Value": "Predicted_Industry_average"}
    )

    # Merge industry average
    projections_df_merged = pd.merge(
        projections_df_merged,
        projections_df_industry_avg[["Business_Vertical", "Metrics", "Predicted_Industry_average"]],
        on=["Business_Vertical", "Metrics"],
        how="left"
    )

    # Cutoff calculation
    cut_off_table = projections_df_merged.copy()
    cut_off_table["Cut_off_value"] = cut_off_table["current*"] * 1.4
    cut_off_table = cut_off_table.drop(
        columns=["Predicted_Actual_Value", "final_factor", "current*", "Predicted_Industry_average"]
    )
    projections_df_merged = projections_df_merged.merge(
        cut_off_table[["Business_Vertical", "Metrics", "Year", "Cut_off_value"]],
        on=["Business_Vertical", "Metrics", "Year"],
        how="left"
    )

    projections_df_merged = projections_df_merged.drop_duplicates()

    # Output to Excel
    projections_df_merged.to_excel(output_path, index=False)
    return projections_df_merged

# Example usage:
# df_simulator = pd.read_excel("Competitor_analysis_anshika.xlsx", sheet_name="simulator")
# df_initiatives = pd.read_excel("Competitor_analysis_anshika.xlsx", sheet_name="Sheet27")
# df_industry_avg = pd.read_excel("Competitor_analysis_anshika.xlsx", sheet_name="industry_average_adjustd")
# result_df = generate_projection_output(df_simulator, df_initiatives, df_industry_avg, "projection_output.xlsx")