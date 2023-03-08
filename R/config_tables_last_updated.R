update_config_tables_last_updated <- function(table_name, date = NULL, datetime = NULL) {
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

  table_cleaned <- stringr::str_split(table_name, "].\\[")[[1]]
  table_cleaned <- table_cleaned[length(table_cleaned)]

  to_upload <- data.table(
    table_name = table_cleaned,
    date = date,
    datetime = datetime
  )
  config$tables$config_tables_last_updated$upsert_data(to_upload)
}


#' get_config_last_updated
#' Gets the config_last_updated db table
#' @param table_name Table name
#' @export
get_config_tables_last_updated <- function(table_name = NULL) {
  if (!is.null(table_name)) {
    temp <- config$tables$config_tables_last_updated$tbl() %>%
      dplyr::filter(table_name == !!table_name) %>%
      dplyr::collect() %>%
      as.data.table()
  } else {
    temp <- config$tables$config_tables_last_updated$tbl() %>%
      dplyr::collect() %>%
      as.data.table()
  }
  return(temp)
}
