import dagster as dg

# 2.4.1 Set the cron_schedule for the job kpi_fy_monthly_job
kpi_fy_monthly_job_schedule = dg.ScheduleDefinition(
    name="kpi_fy_monthly_job",
    target=dg.define_asset_job(name="kpi_fy_monthly_job", selection=dg.AssetSelection.groups("plan")),
    cron_schedule="0 0 3,21 * *",  # run on the 3rd and 21st day of every month at 00:00
    execution_timezone="Asia/Bangkok",  # set timezone as Bangkok
    default_status=dg.DefaultScheduleStatus.RUNNING,
)