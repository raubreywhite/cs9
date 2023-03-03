set_db <- function() {



  # which db is preferred?
  sql <- glue::glue("SELECT name, has_dbaccess(name) FROM sys.databases")
  db_names <- DBI::dbGetQuery(pools$no_db, sql)
  names(db_names) <- c("name", "access")
  db_names <- db_names$name[db_names$access == 1]
  if (config$dbconfigs$restr$db %in% db_names) {
    config$db_config_preferred <- "restr"
  } else {
    config$db_config_preferred <- "anon"
  }

  # create pool connections for all dbs
  for (i in config$dbconfigs) try(create_pool_connection(i, use_db = TRUE), silent = T)

  packageStartupMessage("Preferred sc9 database access level: ", config$db_config_preferred)

  # config_last_updated ----
  add_schema_v8(
    name_access = c("config"),
    name_grouping = "last_updated",
    name_variant = NULL,
    dbconfigs = config$dbconfigs,
    field_types = c(
      "type" = "TEXT",
      "tag" = "TEXT",
      "date" = "DATE",
      "datetime" = "DATETIME"
    ),
    keys = c(
      "type",
      "tag"
    ),
    censors = list(
      config = list()
    ),
    validator_field_types = validator_field_types_blank,
    validator_field_contents = validator_field_contents_blank,
    info = "This db table is used for..."
  )

  # config_data_hash_for_each_plan ----
  add_schema_v8(
    name_access = c("config"),
    name_grouping = "data_hash",
    name_variant = "for_each_plan",
    dbconfigs = config$dbconfigs,
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
    censors = list(
      config = list()
    ),
    validator_field_types = validator_field_types_blank,
    validator_field_contents = validator_field_contents_blank,
    info = "This db table is used for..."
  )
}
