#' Flags/values to be used in the 'dashboards' scene
#' @export config
config <- new.env()

config$is_auto <- FALSE
config$verbose <- FALSE
config$in_parallel <- FALSE
config$tables <- list()

# When running plans in parallel, if a plan fails it is retried five times.
# This lets a user track which attempt they are on.
# This is mostly useful so that emails and smses are only sent when
# cs9::config$plan_attempt_index==1
config$plan_attempt_index <- 1
