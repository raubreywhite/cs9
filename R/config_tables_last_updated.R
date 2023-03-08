update_config_tables_last_updated <- function(table, date = NULL, datetime = NULL) {
  if (is.null(config$tables$config_tasks_stats$conn)) config$tables$config_tasks_stats$connect()

  if (!is.null(datetime)) datetime <- as.character(datetime)

  if (is.null(date) & is.null(datetime)) {
    date <- lubridate::today()
    datetime <- as.character(lubridate::now())
  }
  if (is.null(date) & !is.null(datetime)) {
    date <- stringr::str_sub(datetime, 1, 10)
  }
  if (!is.null(date) & is.null(datetime)) {
    datetime <- paste0(date, " 00:01:00")
  }

  table_cleaned <- stringr::str_split(table, "].\\[")[[1]]
  table_cleaned <- table_cleaned[length(table_cleaned)]

  to_upload <- data.table(
    table = table_cleaned,
    date = date,
    datetime = datetime
  )
  config$tables$config_tasks_stats$upsert_data(to_upload)
}


#' get_config_last_updated
#' Gets the config_last_updated db table
#' @param table Table name
#' @export
get_config_tables_last_updated <- function(table = NULL) {
  if (is.null(config$tables$config_tasks_stats$conn)) config$tables$config_tasks_stats$connect()

  if (!is.null(table)) {
    temp <- config$tables$config_tasks_stats$tbl() %>%
      dplyr::filter(table == !!table) %>%
      dplyr::collect() %>%
      as.data.table()
  } else {
    temp <- config$tables$config_tasks_stats$tbl() %>%
      dplyr::collect() %>%
      as.data.table()
  }
  return(temp)
}
