from dagster import EventRecordsFilter, AssetKey, DagsterEventType

def convert_dataversion_to_int(dv):
    return int(dv.versionNumber)

def get_metadata_version(context):
    """
    A function that takes in the context object and returns int version attached to asset materialization if any for the function latest_version 
    """
    event_records = context.instance.get_event_records(
        EventRecordsFilter(asset_key=AssetKey("latest_version"),
                           event_type=DagsterEventType.ASSET_MATERIALIZATION))
    if len(event_records) > 0:
        return int(event_records[-1].event_log_entry.dagster_event.event_specific_data.materialization.metadata["version"].value)