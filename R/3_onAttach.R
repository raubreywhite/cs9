#' @import data.table
#' @importFrom magrittr %>%
.onAttach <- function(libname, pkgname) {
  version <- tryCatch(
    utils::packageDescription("sc9", fields = "Version"),
    warning = function(w){
      1
    }
  )

  packageStartupMessage(paste0(
    "sc9 ",
    version,
    "\n",
    "https://www.csids.no/sc9/"
  ))
}
