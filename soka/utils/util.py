import os
from datetime import datetime, timedelta
import pandas as pd
from dagster import (
    AssetKey,
    AssetMaterialization,
    DagsterEventType,
    DataVersion,
    EventRecordsFilter,
    Output,
)
from kaggle.api.kaggle_api_extended import KaggleApi


def convert_dataversion_to_int(dv):
    return int(dv.versionNumber)


def get_metadata_version(context, asset_key: str):
    """
    A function that takes in the context object and returns int version attached to asset materialization if any for the function latest_version
    """
    event_records = context.instance.get_event_records(
        EventRecordsFilter(
            asset_key=AssetKey(asset_key),
            event_type=DagsterEventType.ASSET_MATERIALIZATION,
        )
    )
    if len(event_records) > 0:
        return (int(event_records[0].event_log_entry.dagster_event.event_specific_data.materialization.metadata["curr_version"].value))
    else:
        return None



def try_for_version(owner_slug: str, dataset_slug: str, version: int):
    try:
        api = KaggleApi()
        api.authenticate()
        _ = api.datasets_list_files_with_http_info(
            owner_slug, dataset_slug, dataset_version_number=f"{version}"
        )
        return version
    except Exception:
        return


def reset_temp_dir(dir: str):
    for csv in os.listdir(dir):
        if csv.endswith(".csv"):
            os.remove(os.path.join(dir, csv))

def fetch_version(version):
    print("Version:", version)
    yield AssetMaterialization(asset_key="latest_version",
                               description="New fetched version",
                               metadata={"curr_version": version,
                                         "next_version": version+1})
    yield Output(version, data_version=DataVersion(str(version)))


"""
I had built this script to simulate the dim_dates
"""


def dimension_dates(start_date: str = "2021-01-01", end_date: str ="2022-09-06"):
    start = datetime.strptime(f"{start_date}", '%Y-%m-%d').date()
    end  = datetime.strptime(f"{end_date}", '%Y-%m-%d').date()
    diff = (end - start).days
    
    dates = []
    date_ids = []

    for i in range(diff):
        # create dates
        date = start + timedelta(i)
        date_ids.append(datetime.strftime(date, '%Y%m%d'))
        dates.append(date.strftime("%m-%d-%Y"))

    # create dataframe with dates columnn
    df = pd.DataFrame({"date_key": date_ids, 
                       "date": dates}).astype({"date_key": int, "date": "datetime64[s]"})
    
    # df['date'] = pd.to_datetime(df['date'].values, unit="D")

    df["date_description"] = df.date.dt.strftime("%B %-d, %Y")
    # df.date.apply(lambda x: f"{x.year}")

    df["day_of_week"] = df.date.dt.strftime("%A")

    df["day_number_in_calendar_month"] = df.date.dt.strftime("%-d")

    df["day_number_in_calendar_year"] = df.date.dt.strftime("%-j")

    df["calendar_week_number_in_year"] = df.date.dt.strftime("%U")

    df["calendar_month_name"] = df.date.dt.strftime("%B")

    df["calendar_month_number_in_year"] = df.date.dt.strftime("%m")

    df["calendar_year_month"] = df.date.dt.strftime("%Y-%m")

    df["calendar_year"] = df.date.dt.strftime("%Y")

    df["weekday_indicator"] = df.date.apply(lambda x: "Weekday" if x.weekday() < 5 else "Weekend")


    # # create year column
    # df['year_num'] = df.calendar_dt.apply(lambda x: x.year)

    # # create month column
    # df['month_of_the_year_num'] = df.calendar_dt.apply(lambda x: x.month)

    # # create month day column
    # df['day_of_the_month_num'] = df.calendar_dt.apply(lambda x: x.day)

    # # create week day column
    # df['day_of_the_week_num'] = df.calendar_dt.apply(lambda x: x.weekday())

    # create working day column using the holiday package
    # df['working_day'] = df.calendar_dt.apply(lambda x: x in nl_holidays)

    # df.to_csv("./data/dates.csv", index=False)

    return df
