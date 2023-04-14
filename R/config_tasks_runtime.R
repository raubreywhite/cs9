update_config_tasks_stats <- function(
    task,
    implementation_version = "unspecified",
    cores_n,
    plans_n,
    analyses_n,
    start_datetime,
    stop_datetime,
    ram_all_cores_mb,
    ram_per_core_mb,
    status
) {
  stopifnot(status %in% c("succeeded", "failed"))

  start_datetime <- as.character(start_datetime)
  start_date <- stringr::str_sub(start_datetime, 1, 10)

  stop_datetime <- as.character(stop_datetime)
  stop_date <- stringr::str_sub(stop_datetime, 1, 10)

  runtime_minutes <- round(as.numeric(difftime(stop_datetime, start_datetime, units = "min")), 2)

  to_upload <- data.table(
    task = task,
    sc_version = utils::packageDescription("sc9", fields = "Version"),
    implementation_version = implementation_version,
    cores_n = cores_n,
    plans_n = plans_n,
    analyses_n = analyses_n,
    start_date = start_date,
    start_datetime = start_datetime,
    stop_date = stop_date,
    stop_datetime = stop_datetime,
    runtime_minutes = runtime_minutes,
    ram_all_cores_mb = ram_all_cores_mb,
    ram_per_core_mb = ram_per_core_mb,
    status = status
  )
  config$tables$config_tasks_stats$upsert_data(to_upload)
}

#' get_config_last_updated
#' Gets the config_last_updated db table
#' @param task Task name
#' @param last_run Just get the last run?
#' @export
get_config_tasks_stats <- function(task = NULL, last_run = FALSE) {
  if (!is.null(task)) {
    temp <- config$tables$config_tasks_stats$tbl() %>%
      dplyr::filter(task == !!task) %>%
      dplyr::collect() %>%
      as.data.table()
  } else {
    temp <- config$tables$config_tasks_stats$tbl() %>%
      dplyr::collect() %>%
      as.data.table()
  }
  return(temp)
}
