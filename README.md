# cs9 <a href="https://www.csids.no/cs9/"><img src="man/figures/logo.png" align="right" width="120" /></a>


## Overview 

[Surveillance Core 9](https://www.csids.no/cs9/) ("cs9") is a free and open-source framework for real-time analysis and disease surveillance.

Read the introduction vignette [here](https://www.csids.no/cs9/articles/cs9.html) or run `help(package="cs9")`.

## csverse

<a href="https://www.csids.no/packages.html"><img src="https://www.csids.no/packages/csverse.png" align="right" width="120" /></a>

The [csverse](https://www.csids.no/packages.html) is a set of R packages developed to help solve problems that frequently occur when performing disease surveillance.

If you want to install the dev versions (or access packages that haven't been released on CRAN), run `usethis::edit_r_profile()` to edit your `.Rprofile`. 

Then write in:

```
options(
  repos = structure(c(
    CSVERSE = "https://www.csids.no/drat/",
    CRAN    = "https://cran.rstudio.com"
  ))
)
```

Save the file and restart R.

You can now install [csverse](https://www.csids.no/packages.html) packages from our [drat repository](https://www.csids.no/drat/).

```
install.packages("cs9")
```
