import dagster as dg

# Set the cron_schedule for the job bg020_daily_job
bg020_daily_job = dg.ScheduleDefinition(
    name="bg020_daily_job",
    target=dg.define_asset_job(name="bg020_daily_job", selection=dg.AssetSelection.groups("bg020")),
    cron_schedule="0 21 * * *",  # run every day at 21:00
    execution_timezone="Asia/Bangkok",  # set timezone as Bangkok
    default_status=dg.DefaultScheduleStatus.RUNNING,
)