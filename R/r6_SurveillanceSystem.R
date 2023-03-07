#' Surveillance System
#'
#' An R6 Action class contains a function called 'run' that takes three arguments:
#' - data (list)
#' - arg (list)
#' - schema (list)
#'
#' An action is (note: we dont explicitly create these):
#' - one R6 Action class
#' - A plnr::Plan that provide
#'   a) data (from the plan -- `get_data`)
#'   b) arguments (from the plan -- `get_argset`)
#' to the R6 action class
#'
#' A task is:
#' - one R6 Action class
#' - A plnr::Plan that provide
#'   a) data (from the plan -- `get_data`)
#'   b) arguments (from the plan -- `get_argset`)
#' to the R6 action class
#'
#' A TaskManager is:
#' - one R6 Action class
#' - A list of plnr::Plan's
#'
#' @import R6
#' @export SurveillanceSystem_v9
SurveillanceSystem_v9 <- R6::R6Class(
  "SurveillanceSystem_v9",
  public = list(
    tables = list(),
    tasks = list(),
    initialize = function() {
      # nothing
    },
    add_table = function(
      name_access,
      name_grouping = NULL,
      name_variant = NULL,
      field_types,
      keys,
      indexes = NULL,
      validator_field_types = csdb::validator_field_types_blank,
      validator_field_contents = csdb::validator_field_contents_blank
      ){
      force(name_access)
      force(name_grouping)
      force(name_variant)
      force(field_types)
      force(keys)
      force(indexes)
      force(validator_field_types)
      force(validator_field_contents)

      table_name <- paste0(c(name_access, name_grouping, name_variant), collapse = "_")

      dbtable <- csdb::DBTable_v9$new(
        dbconfig = config$dbconfigs[[name_access]],
        table_name = table_name,
        field_types = field_types,
        keys = keys,
        indexes = indexes,
        validator_field_types = validator_field_types,
        validator_field_contents = validator_field_contents
      )

      self$tables[[table_name]] <- dbtable
    },
    #' add_task_from_config
    #' @param name_grouping Name of the task (grouping)
    #' @param name_action Name of the task (action)
    #' @param name_variant Name of the task (variant)
    #' @param cores Number of CPU cores
    #' @param permission A permission R6 instance
    #' @param plan_analysis_fn_name The name of a function that returns a named list \code{list(for_each_plan = list(), for_each_analysis = NULL)}.
    #' @param for_each_plan A list, where each unit corresponds to one data extraction. Generally recommended to use \code{plnr::expand_list}.
    #' @param for_each_analysis A list, where each unit corresponds to one analysis within a plan (data extraction). Generally recommended to use \code{plnr::expand_list}.
    #' @param universal_argset A list, where these argsets are applied to all analyses univerally
    #' @param upsert_at_end_of_each_plan Do you want to upsert your results automatically at the end of each plan?
    #' @param insert_at_end_of_each_plan Do you want to insert your results automatically at the end of each plan?
    #' @param action_fn_name The name of the function that will be called for each analysis with arguments \code{data}, \code{argset}, \code{schema}
    #' @param data_selector_fn_name The name of a function that will be called to obtain the data for each analysis. The function must have the arguments \code{argset}, \code{schema} and must return a named list.
    #' @param tables A named list that maps \code{sc9::config$schemas} for use in \code{action_fn_name} and \code{data_selector_fn_name}
    #' @export
    add_task = function(
      name_grouping = NULL,
      name_action = NULL,
      name_variant = NULL,
      cores = 1,
      permission = NULL,
      plan_analysis_fn_name = NULL,
      for_each_plan = NULL,
      for_each_analysis = NULL,
      universal_argset = NULL,
      upsert_at_end_of_each_plan = FALSE,
      insert_at_end_of_each_plan = FALSE,
      action_fn_name,
      data_selector_fn_name = NULL,
      tables = NULL
      ) {
        force(name_grouping)
        force(name_action)
        force(name_variant)
        force(cores)
        force(permission)
        force(plan_analysis_fn_name)
        force(for_each_plan)
        force(for_each_analysis)
        force(universal_argset)
        force(upsert_at_end_of_each_plan)
        force(insert_at_end_of_each_plan)
        force(action_fn_name)
        force(data_selector_fn_name)

        task <- task_from_config_v9(
          name_grouping = name_grouping,
          name_action = name_action,
          name_variant = name_variant,
          cores = cores,
          permission = permission,
          plan_analysis_fn_name = plan_analysis_fn_name,
          for_each_plan = for_each_plan,
          for_each_analysis = for_each_analysis,
          universal_argset = universal_argset,
          upsert_at_end_of_each_plan = upsert_at_end_of_each_plan,
          insert_at_end_of_each_plan = insert_at_end_of_each_plan,
          action_fn_name = action_fn_name,
          data_selector_fn_name = data_selector_fn_name,
          tables = tables
        )
        self$tasks[[task$name]] <- task
    },
    shortcut_get_task = function(task_name){
      retval <- self$tasks[[task_name]]
      retval$update_plans()
      retval
    },
    shortcut_get_tables = function(task_name){
      self$shortcut_get_task(task_name)$tables
    },
    shortcut_get_argset = function(task_name, index_plan = 1, index_analysis = 1){
      self$shortcut_get_task(task_name)$plans[[index_plan]]$get_argset(index_analysis)
    },
    shortcut_get_data = function(task_name, index_plan = 1){
      self$shortcut_get_task(task_name)$plans[[index_plan]]$get_data()
    },
    shortcut_get_plans_argsets_as_dt = function(task_name){
      plans <- self$shortcut_get_task(task_name)$plans

      retval <- lapply(plans, function(x) analyses_to_dt(x$analyses))
      retval <- rbindlist(retval)
      setcolorder(retval, c("index_plan", "index_analysis"))
      retval
    },
    shortcut_run_task = function(task_name){
      task <- self$shortcut_get_task(task_name)
      task$run(log = FALSE)
    }
  )
)

#' Run
#' @param task_name Task name
#' @param ss_prefix The prefix that locates the surveillance system
#' @export
run_task_sequentially_as_rstudio_job_loading_from_devtools <- function(
    task_name,
    ss_prefix = "global$ss"
    ){
  tempfile <- fs::path(tempdir(check = T), paste0(task_name, ".R"))
  cat(glue::glue(
    "
        devtools::load_all('.')
        {ss_prefix}$tasks[['{task_name}']]$cores <- 1
        {ss_prefix}$shortcut_run_task('{task_name}')
      "
  ), file = tempfile)
  rstudioapi::jobRunScript(
    path = tempfile,
    name = task_name,
    workingDir = getwd(),
  )
}

generic_data_function_factory_v9 <- function(tables, argset, fn_name) {
  force(tables)
  force(argset)
  force(fn_name)
  function() {
    for (i in tables) i$connect()
    on.exit(for (i in tables) i$disconnect())
    fn <- plnr::get_anything(fn_name)
    fn(argset, tables)
  }
}

generic_list_plan_function_factory_v9 <- function(universal_argset,
                                                  tables,
                                                  plan_analysis_fn_name,
                                                  action_fn_name,
                                                  data_selector_fn_name) {
  force(universal_argset)
  force(tables)
  force(plan_analysis_fn_name)
  force(action_fn_name)
  force(data_selector_fn_name)

  function() {
    for (i in tables) i$connect()
    on.exit(for (i in tables) i$disconnect())

    fn <- plnr::get_anything(plan_analysis_fn_name)
    plan_analysis <- fn(universal_argset, tables)
    list_plan <- task_from_config_v9_list_plan(
      for_each_plan = plan_analysis$for_each_plan,
      for_each_analysis = plan_analysis$for_each_analysis,
      universal_argset = universal_argset,
      action_fn_name = action_fn_name,
      data_selector_fn_name = data_selector_fn_name,
      tables = tables
    )
    return(list_plan)
  }
}

task_from_config_v9_list_plan <- function(for_each_plan,
                                          for_each_analysis = NULL,
                                          universal_argset = NULL,
                                          action_fn_name,
                                          data_selector_fn_name,
                                          tables) {
  index <- 1
  list_plan <- list()

  for (index_plan in seq_along(for_each_plan)) {
    # create a new plan
    list_plan[[length(list_plan) + 1]] <- plnr::Plan$new()

    # add data
    argset <- c(
      "**universal**" = "*",
      universal_argset,
      "**plan**" = "*",
      for_each_plan[[index_plan]],
      "**automatic**" = "*",
      index = index
    )
    argset$today <- lubridate::today()
    argset$yesterday <- lubridate::today() - 1

    if (!is.null(data_selector_fn_name)) {
      list_plan[[length(list_plan)]]$add_data(
        name = "data__________go_up_one_level",
        fn = generic_data_function_factory_v9(
          tables = tables,
          argset = argset,
          fn_name = data_selector_fn_name
        )
      )
    }

    # add analyses
    if (is.null(for_each_analysis)) {
      for_each_analysis <- list(NULL)
    }
    for (index_analysis in seq_along(for_each_analysis)) {
      # add analysis
      argset <- c(
        "**universal**" = "*",
        universal_argset,
        "**plan**" = "*",
        for_each_plan[[index_plan]],
        "**analysis**" = "*",
        for_each_analysis[[index_analysis]],
        "**automatic**" = "*",
        index = index
      )
      argset$today <- lubridate::today()
      argset$yesterday <- lubridate::today() - 1

      list_plan[[length(list_plan)]]$add_analysis_from_list(
        fn_name = action_fn_name,
        l = list(argset)
      )

      index <- index + 1
    }
  }

  return(list_plan)
}

task_from_config_v9 <- function(
  name_grouping = NULL,
  name_action = NULL,
  name_variant = NULL,
  cores = 1,
  permission = NULL,
  plan_analysis_fn_name = NULL,
  for_each_plan = NULL,
  for_each_analysis = NULL,
  universal_argset = NULL,
  upsert_at_end_of_each_plan = FALSE,
  insert_at_end_of_each_plan = FALSE,
  action_fn_name,
  data_selector_fn_name = NULL,
  tables = NULL
  ) {
  if (is.null(for_each_plan) & is.null(plan_analysis_fn_name)) stop("You must provide at least one of for_each_plan or plan_analysis_fn_name")

  if (!is.null(for_each_plan)) {
    stopifnot(is.list(for_each_plan))
    list_plan <- task_from_config_v9_list_plan(
      for_each_plan = for_each_plan,
      for_each_analysis = for_each_analysis,
      universal_argset = universal_argset,
      action_fn_name = action_fn_name,
      data_selector_fn_name = data_selector_fn_name,
      tables = tables
    )
    update_plans_fn <- NULL
  } else if (!is.null(plan_analysis_fn_name)) {
    list_plan <- NULL
    update_plans_fn <- generic_list_plan_function_factory_v9(
      universal_argset = universal_argset,
      tables = tables,
      plan_analysis_fn_name = plan_analysis_fn_name,
      action_fn_name = action_fn_name,
      data_selector_fn_name = data_selector_fn_name
    )
  }

  task <- Task$new(
    name_grouping = name_grouping,
    name_action = name_action,
    name_variant = name_variant,
    permission = permission,
    plans = list_plan,
    update_plans_fn = update_plans_fn,
    tables = tables,
    cores = cores,
    upsert_at_end_of_each_plan = upsert_at_end_of_each_plan,
    insert_at_end_of_each_plan = insert_at_end_of_each_plan
  )

  return(task)
}


