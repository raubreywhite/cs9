DBPartitionedTableExtended_v9 <- R6::R6Class(
  "DBPartitionedTableExtended_v9",
  public = list(
    tables = list(),
    partitions = c(),
    column_name_partition = "",
    initialize = function(
    dbconfig,
    table_name_base,
    table_name_partitions,
    column_name_partition,
    field_types,
    keys,
    indexes = NULL,
    validator_field_types = validator_field_types_blank,
    validator_field_contents = validator_field_contents_blank
    ) {
      force(table_name_partitions)
      self$partitions <- table_name_partitions

      force(column_name_partition)
      self$column_name_partition <- column_name_partition

      # ensure that partition name is last in dataset
      if(column_name_partition %in% names(field_types)){
        field_types <- field_types[!names(field_types) == column_name_partition]
      }
      field_types <- c(field_types, "TEXT")
      names(field_types)[length(field_types)] <- column_name_partition

      self$tables <- vector("list", length(self$partitions))
      names(self$tables) <- self$partitions
      for(i in self$partitions){
        table_name <- paste0(c(table_name_base,"PARTITION", i), collapse = "_")

        dbtable <- DBTableExtended_v9$new(
          dbconfig = dbconfig,
          table_name = table_name,
          field_types = field_types,
          keys = keys,
          indexes = indexes,
          validator_field_types = validator_field_types,
          validator_field_contents = validator_field_contents
        )
        self$tables[[i]] <- dbtable
      }
    },
    insert_data = function(newdata, verbose = TRUE){
      private$check_for_correct_partitions_in_data(newdata)

      partitions_in_use <- unique(newdata[[self$column_name_partition]])
      partitions_in_use <- sample(partitions_in_use, length(partitions_in_use)) # randomize order
      for(i in partitions_in_use){
        index <- newdata[[self$column_name_partition]] == i
        self$tables[[i]]$insert_data(newdata[index,], verbose)
      }
    },
    upsert_data = function(newdata, drop_indexes = names(self$indexes), verbose = TRUE){
      private$check_for_correct_partitions_in_data(newdata)

      partitions_in_use <- unique(newdata[[self$column_name_partition]])
      partitions_in_use <- unique(newdata[[self$column_name_partition]])
      for(i in partitions_in_use){
        index <- newdata[[self$column_name_partition]] == i
        self$tables[[i]]$upsert_data(newdata[index,], drop_indexes, verbose)
      }
    },
    drop_all_rows = function(){
      for(i in self$partitions_randomized){
        self$tables[[i]]$drop_all_rows()
      }
    },
    drop_rows_where = function(condition){
      for(i in self$partitions_randomized){
        self$tables[[i]]$drop_rows_where(condition)
      }
    },
    keep_rows_where = function(condition){
      for(i in self$partitions_randomized){
        self$tables[[i]]$keep_rows_where(condition)
      }
    },
    drop_all_rows_and_then_upsert_data = function(newdata, drop_indexes = names(self$indexes), verbose = TRUE) {
      private$check_for_correct_partitions_in_data(newdata)

      for(i in self$partitions_randomized){
        index <- self[[self$column_name_partition]] == i
        self$tables[[i]]$drop_all_rows_and_then_upsert_data(newdata[index,], drop_indexes, verbose)
      }
    },
    drop_all_rows_and_then_insert_data = function(newdata, verbose = TRUE) {
      private$check_for_correct_partitions_in_data(newdata)

      for(i in self$partitions_randomized){
        index <- newdata[[self$column_name_partition]] == i
        self$tables[[i]]$drop_all_rows_and_then_insert_data(newdata[index,], verbose)
      }
    },
    remove_table = function(){
      for(i in self$partitions_randomized){
        self$tables[[i]]$remove_table()
      }
    },
    drop_indexes = function(){
      for(i in self$partitions_randomized){
        self$tables[[i]]$drop_indexes()
      }
    },
    add_indexes = function(){
      for(i in self$partitions_randomized){
        self$tables[[i]]$add_indexes()
      }
    }
  ),
  active = list(
    # sometimes we want this in a randomized order, so that the SQL table isnt locked and blocked
    partitions_randomized = function(){
      sample(self$partitions, length(self$partitions))
    }
  ),
  private = list(
    check_for_correct_partitions_in_data = function(newdata){
      partitions_in_use <- unique(newdata[[self$column_name_partition]])
      if(sum(!partitions_in_use %in% self$partitions)){
        stop("Some partitions exist in the data that do not exist in the partion")
      }
    }
  )
)
