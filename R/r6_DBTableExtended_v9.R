DBTableExtended_v9 <- R6::R6Class(
  "DBTableExtended_v9",
  inherit = csdb::DBTable_v9,
  public = list(
    insert_data = function(newdata, verbose = TRUE){
      super$insert_data(newdata, verbose)
      update_config_tables_last_updated(table_name = self$table_name)
    },
    upsert_data = function(newdata, drop_indexes = names(self$indexes), verbose = TRUE){
      super$upsert_data(newdata, drop_indexes, verbose)
      update_config_tables_last_updated(table_name = self$table_name)
    },
    drop_all_rows = function(){
      super$drop_all_rows()
      update_config_tables_last_updated(table_name = self$table_name)
    },
    drop_rows_where = function(condition){
      super$drop_rows_where(condition)
      update_config_tables_last_updated(table_name = self$table_name)
    },
    keep_rows_where = function(condition){
      super$keep_rows_where(condition)
      update_config_tables_last_updated(table_name = self$table_name)
    },
    drop_all_rows_and_then_upsert_data = function(newdata, drop_indexes = names(self$indexes), verbose = TRUE) {
      super$drop_all_rows_and_then_upsert_data(newdata, drop_indexes, verbose)
      update_config_tables_last_updated(table_name = self$table_name)
    },
    drop_all_rows_and_then_insert_data = function(newdata, verbose = TRUE) {
      super$drop_all_rows_and_then_insert_data(newdata, verbose)
      update_config_tables_last_updated(table_name = self$table_name)
    }
  )
)
