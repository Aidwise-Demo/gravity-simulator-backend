import clickhouse_connect
from fastapi import FastAPI, Request, HTTPException
from fastapi.encoders import jsonable_encoder
import time
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import Dict, List, Optional
from pydantic import BaseModel
from simulator import generate_projection_output
import pandas as pd
from Status_Logic import status_calculation
class FilterRequest(BaseModel):
    metric: Optional[str] = None
    period: Optional[str] = None
    businessVertical: Optional[str] = None
    targetValue: Optional[float] = None




app = FastAPI()

origins = [
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def db_connection_middleware(request: Request, call_next):
    response = await call_next(request)
    return response




# Establish a connection
client = clickhouse_connect.get_client(
    host='4.247.30.213',      # or your ClickHouse server IP
    port=8125,             # default HTTP port
    username='sdzdev',
    password='Gravity@123',
    database='Competitors_database'
)

second_client = clickhouse_connect.get_client(
    host='dhtest-ai-clickhouse.strategydotzero.com',
    port=8123,
    username='sdzdev',
    password='Gravity@123',
    database='Etl'
)

initiative_query = """SELECT distinct
                        COALESCE(s.SectionName, b.BranchName, d.DivisionName, 'N/A') AS Business_Vertical,
                        proj.ProjectName AS Initiative,
                        year(proj.StartDate) AS `Start Year`,
                        year(proj.EndDate) AS `End Year`,
                        projstat.Name AS Status
                    FROM Etl.InterimBenefits ib
                    LEFT JOIN (
                        SELECT *
                        FROM Etl.BenefitRealisationsSchedule
                        WHERE Status <> 0
                    ) brs ON ib.Id = brs.InterimBenefitID
                    LEFT JOIN (
                        SELECT
                            Id, DivisionId, BranchId, SectionId,
                            arrayJoin(InterimBenefitIds) AS InterimBenefitId_Val
                        FROM Etl.ProjectFact
                    ) pfib ON ib.Id = pfib.InterimBenefitId_Val
                    LEFT JOIN (
                        SELECT
                            Id, DivisionId,
                            arrayJoin(InterimBenefitIds) AS InterimBenefitId_Val,
                            arrayJoin(BusinessPlanObjectivesIds) AS BPObjID_Val
                        FROM Etl.ProjectFact
                    ) pfbp ON ib.Id = pfbp.InterimBenefitId_Val
                    LEFT JOIN (
                        SELECT *
                        FROM Etl.Division
                        WHERE Status <> 0
                    ) d ON pfib.DivisionId = d.Id
                    LEFT JOIN (
                        SELECT *
                        FROM Etl.Branch
                        WHERE Status <> 0
                    ) b ON pfib.BranchId = b.Id
                    LEFT JOIN (
                        SELECT *
                        FROM Etl.Section
                        WHERE Status <> 0
                    ) s ON pfib.SectionId = s.Id
                    LEFT JOIN (
                        SELECT *
                        FROM Etl.BenefitPerformanceMeasurement
                        WHERE Status <> 0
                    ) bpm ON ib.Id = bpm.InterimBenefitId
                    LEFT JOIN (
                        SELECT *
                        FROM Etl.StrategicandBusinessPlanStatus
                        WHERE Status <> 0
                    ) sbp ON bpm.CurrentStatus = sbp.Id
                    LEFT JOIN (
                        SELECT *
                        FROM Etl.BusinessPlanObjective
                        WHERE Status <> 0
                          AND BusinessPlanId IN (
                              SELECT Id FROM Etl.BusinessPlan WHERE Status <> 0
                          )
                    ) bpo ON bpo.Id = pfbp.BPObjID_Val
                        AND (
                            (pfib.SectionId = 0 AND pfib.BranchId > 0 AND pfib.DivisionId = 0 AND bpo.ParentObjectiveId = 0)
                            OR
                            (pfib.SectionId > 0 OR pfib.DivisionId > 0)
                        )
                    LEFT JOIN (
                        SELECT *
                        FROM Etl.StrategicObjective
                    ) so ON so.Id = bpo.ParentObjectiveId
                    LEFT JOIN (
                        SELECT
                            Id,
                            Etl.Project.ProjectName,
                            Etl.Project.OwnerName,
                            Etl.Project.ManagerName,
                            Etl.Project.ProjectStatus,
                            Etl.Project.StartDate,
                            Etl.Project.EndDate  
                        FROM Etl.Project
                        WHERE Status <> 0
                    ) proj ON proj.Id = pfib.Id
                    LEFT JOIN (
                        SELECT
                            Etl.ProjectStatus.ConfigurationValue,
                            Etl.ProjectStatus.ColorHex,
                            Etl.ProjectStatus.Name
                        FROM Etl.ProjectStatus
                    ) projstat ON proj.ProjectStatus = projstat.ConfigurationValue
                    WHERE
                        ib.Status <> 0
                        AND so.ObjectiveName = 'Maximize Earnings';"""

# Convert to list of dicts
def df_to_json_format(df):
    output = []
    df = df.fillna("NA")
    for _, row in df.iterrows():
        entry = {
            "name": row["Business_Vertical"],
            "predictedTarget": row["target_value"],
            "current": row["Predicted_Actual_Value"],
            "currentStar": row["current*"],
            "industryAverage": row["Predicted_Industry_average"],
            "cutoff": row["Cut_off_value"],
            "status": row["expected_risk"]
        }
        output.append(entry)
    return {"businessVerticalTargets": output}

def format_trend_analysis(df, df2=None):
    result = {
        "trendAnalysis": {
            "overall": {
                "quarters": df["Quarter"].tolist(),
                "actualValues": df["QuarterlyActualSum"].tolist(),
                "targetValues": df["QuarterlyTargetSum"].tolist()
            }
        }
    }

    if df2 is not None:
        result["trendAnalysis"]["businessVerticals"] = {
            "quarters": df2["Quarter"].tolist(),
            "actualValues": df2["QuarterlyActualSum"].tolist(),
            "targetValues": df2["QuarterlyTargetSum"].tolist()
        }

    return result




def format_summary(df):
    row = df.iloc[0]
    actual = row["Actual"]
    target = row["Target"]

    # Avoid divide-by-zero error
    if target != 0:
        achievement = f"{round((actual / target) * 100)}%"
    else:
        achievement = "N/A"

    return {
        "summary": {
            "overallValue": row["Overall_Target"],
            "actualValue": actual,
            "targetValue": target,
            "achievementStatus": achievement
        }
    }


@app.post("/api/gravity/simulation")
async def get_most_popular_buyers(request: FilterRequest):
    quarter = request.period
    metric = request.metric
    businessVertical = request.businessVertical
    target = request.targetValue

    if quarter is None:
        quarter = "Q1 2025"

    if metric is None:
        metric = "EBITDA"

    overallQuery = f"""SELECT
        concat('Q', toString(quarter), ' ', toString(year)) AS Quarter,
        round(sum(Actual_Value), 0) AS QuarterlyActualSum,
        round(sum(Target_Value), 0) AS QuarterlyTargetSum
    FROM (
        SELECT
            toYear(`Measure Date`) AS year,
            toQuarter(`Measure Date`) AS quarter,
            Actual_Value,
            Target_Value
        FROM metrics_data_from_bu
        WHERE Company = ' DH' AND Metrics LIKE '{metric}'
    ) AS sub
    GROUP BY year, quarter
    ORDER BY year, quarter
    """
    vertical_trend_df = None
    if businessVertical is not None:

        verticalQuery = f"""SELECT
            concat('Q', toString(quarter), ' ', toString(year)) AS Quarter,
            round(sum(Actual_Value), 0) AS QuarterlyActualSum,
            round(sum(Target_Value), 0) AS QuarterlyTargetSum
        FROM (
            SELECT
                toYear(`Measure Date`) AS year,
                toQuarter(`Measure Date`) AS quarter,
                Actual_Value,
                Target_Value
            FROM metrics_data_from_bu
            WHERE Company = ' DH' AND Metrics LIKE '{metric}' AND Business_Vertical LIKE '%{businessVertical}%'
        ) AS sub
        GROUP BY year, quarter
        ORDER BY year, quarter
        """
        vertical_trend = client.query(verticalQuery)
        vertical_trend_df = pd.DataFrame(vertical_trend.result_rows, columns=vertical_trend.column_names)

    overallTarget = f"""SELECT
                            round(SUMIf(Actual_Value, toYear(`Measure Date`) = 2025 AND toQuarter(`Measure Date`) = 1), 0) AS Actual,
                            round(SUMIf(Target_Value, toYear(`Measure Date`) = 2025 AND toQuarter(`Measure Date`) = 1), 0) AS Target,
                            round(SUMIf(Target_Value, toYear(`Measure Date`) = 2025 AND toQuarter(`Measure Date`) = 4), 0) AS Overall_Target
                        FROM metrics_data_from_bu
                        WHERE Company = ' DH' AND Metrics LIKE '{metric}'
                        """
    overall_target = client.query(overallTarget)
    overall_target_df = pd.DataFrame(overall_target.result_rows, columns=overall_target.column_names)
    summary_json = format_summary(overall_target_df)

    # Extract quarter and year
    qtr, year = quarter.upper().split()  # qtr = 'Q1', year = '2025'
    quarter_num = int(qtr[1])  # Convert 'Q1' to 1

    # Now dynamically construct the SQL query
    target_query = f"""
        SELECT
            Business_Vertical,
            ROUND(SUM(Target_Value), 0) AS target_value
        FROM metrics_data_from_bu
        WHERE
            toYear(`Measure Date`) = {year}
            AND toQuarter(`Measure Date`) = {quarter_num}
            AND Company = ' DH'
            AND Metrics LIKE '{metric}'
        GROUP BY Business_Vertical
        ORDER BY target_value DESC
    """

    industry_average_query = "select * from simulator_industry_average"

    simulation_query = "select * from simulator_values"

    overall_trend = client.query(overallQuery)
    overall_trend_df = pd.DataFrame(overall_trend.result_rows, columns=overall_trend.column_names)

    trend_json = format_trend_analysis(overall_trend_df, vertical_trend_df)

    simulation_result = client.query(simulation_query)
    simulation_df = pd.DataFrame(simulation_result.result_rows, columns=simulation_result.column_names)

    industry_average_result = client.query(industry_average_query)
    industry_average_df = pd.DataFrame(industry_average_result.result_rows, columns=industry_average_result.column_names)

    initiative_result = second_client.query(initiative_query)
    initiative_df = pd.DataFrame(initiative_result.result_rows, columns=initiative_result.column_names)

    target_result = client.query(target_query)
    target_df = pd.DataFrame(target_result.result_rows, columns=target_result.column_names)

    print("Hi", target, businessVertical)

    if target is not None and businessVertical is not None:
        target_df.loc[target_df['Business_Vertical'] == businessVertical, 'target_value'] = target
    simulated_result_df = generate_projection_output(df_simulator=simulation_df, df_initiatives=initiative_df, df_industry_avg=industry_average_df)

    simulated_result_df = simulated_result_df[simulated_result_df['Metrics'] == metric]
    simulated_result_df = status_calculation(simulated_result_df)
    # Merge target_df into simulated_result_df on Business_Vertical
    simulated_result_df = simulated_result_df.merge(
        target_df[['Business_Vertical', 'target_value']],
        on='Business_Vertical',
        how='left'  # or 'inner' if you only want rows with matching Business_Vertical
    )
    # Sum of 'target_value' excluding NaN
    target_sum = simulated_result_df['target_value'].sum(skipna=True)

    # Total number of rows
    row_count = len(simulated_result_df)

    # Count of rows where expected_risk is "High"
    high_risk_count = (simulated_result_df['expected_risk'] == "High").sum()
    targets_ratio = f"{high_risk_count}/{row_count}"

    result_json = df_to_json_format(simulated_result_df)

    final_json = {
        "company": "Dubai Holdings",
        "period": quarter,
        "industryBenchmark": "Similar Competitors",
        "metric": metric,
        **summary_json,
        **trend_json,
        **result_json,
        "overallScore": {
            "scorePercent": target_sum,
            "targetsRatio": targets_ratio
        },
    }

    print(simulated_result_df)
    return final_json
# Fetch results
# print(result.result_rows)
