.onLoad <- function(libname, pkgname) {

  set_env_vars()
  set_progressr()
  set_plnr()

  # config_tables_last_updated ----
  config$tables$config_tables_last_updated <- csdb::DBTable_v9$new(
    dbconfig = config$dbconfigs$config,
    table_name = "config_tables_last_updated",
    field_types = c(
      "table_name" = "TEXT",
      "date" = "DATE",
      "datetime" = "DATETIME"
    ),
    keys = c(
      "table_name"
    ),
    validator_field_types = csdb::validator_field_types_blank,
    validator_field_contents = csdb::validator_field_contents_blank
  )

  # config_tasks_stats ----
  config$tables$config_tasks_stats <- csdb::DBTable_v9$new(
    dbconfig = config$dbconfigs$config,
    table_name = "config_tasks_stats",
    field_types = c(
      "task" = "TEXT",
      "sc_version" = "TEXT",
      "implementation_version" = "TEXT",
      "cores_n" = "INTEGER",
      "plans_n" = "INTEGER",
      "analyses_n" = "INTEGER",
      "start_date" = "DATE",
      "start_datetime" = "DATETIME",
      "stop_date" = "DATE",
      "stop_datetime" = "DATETIME",
      "runtime_minutes" = "DOUBLE",
      "ram_all_cores_mb" = "DOUBLE",
      "ram_per_core_mb" = "DOUBLE",
      "status" = "TEXT"
    ),
    keys = c(
      "task",
      "start_datetime"
    ),
    validator_field_types = csdb::validator_field_types_blank,
    validator_field_contents = csdb::validator_field_contents_blank
  )

  # config_data_hash_for_each_plan ----
  config$tables$config_data_hash_for_each_plan <- csdb::DBTable_v9$new(
    dbconfig = config$dbconfigs$config,
    table_name = "config_data_hash_for_each_plan",
    field_types = c(
      "task" = "TEXT",
      "index_plan" = "INTEGER",
      "element_tag" = "TEXT",
      "date" = "DATE",
      "datetime" = "DATETIME",
      "element_hash" = "TEXT",
      "all_hash" = "TEXT"
    ),
    keys = c(
      "task",
      "index_plan",
      "element_tag",
      "date",
      "datetime"
    ),
    validator_field_types = csdb::validator_field_types_blank,
    validator_field_contents = csdb::validator_field_contents_blank
  )

  invisible()
}

# Environmental variables ----
get_db_acess_from_env <- function() {
  retval <- Sys.getenv("SC9_DBCONFIG_ACCESS") |>
    stringr::str_split("/") |>
    unlist()
  retval <- retval[retval != ""]
  return(retval)
}

get_db_from_env <- function(access) {

  retval <- list(
    access = access,
    driver = Sys.getenv("SC9_DBCONFIG_DRIVER"),
    port = as.integer(Sys.getenv("SC9_DBCONFIG_PORT")),
    user = Sys.getenv("SC9_DBCONFIG_USER"),
    password = Sys.getenv("SC9_DBCONFIG_PASSWORD"),
    trusted_connection = Sys.getenv("SC9_DBCONFIG_TRUSTED_CONNECTION"),
    sslmode = Sys.getenv("SC9_DBCONFIG_SSLMODE"),
    server = Sys.getenv("SC9_DBCONFIG_SERVER"),
    schema = Sys.getenv(paste0("SC9_DBCONFIG_SCHEMA_", toupper(access))),
    db = Sys.getenv(paste0("SC9_DBCONFIG_DB_", toupper(access)))
  )

  retval$schema <- gsub("\\\\", "\\\\", retval$schema)

  retval$id <- paste0("[", retval$db, "].[", retval$schema, "]")

  return(retval)
}

set_env_vars <- function(){
  config$dbconfigs <- list()
  for (i in get_db_acess_from_env()) {
    config$dbconfigs[[i]] <- get_db_from_env(i)
  }

  if (Sys.getenv("SC9_AUTO") == "1") {
    config$is_auto <- TRUE
  }

  config$path <- Sys.getenv("SC9_PATH")
}

set_progressr <- function() {
  options("progressr.enable" = TRUE)
  progressr::handlers(
    progressr::handler_progress(
      format = "[:bar] :current/:total (:percent) in :elapsedfull, eta: :eta\n",
      clear = FALSE
    )
  )
}

set_plnr <- function() {
  plnr::set_opts(force_verbose = TRUE)
}
