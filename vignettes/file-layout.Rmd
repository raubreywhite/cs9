# File Layout

```{r, include=FALSE}
knitr::opts_chunk$set(collapse = TRUE, warning=FALSE)
options(knitr.kable.NA = '')

unloadNamespace("sc")
library(data.table)
library(magrittr)
library(sc)
```

## Introduction

Implementing Sykdomspulsen Core requires a number of functions to be called in the correct order. To make this as simple as possible, we have provided a skeleton implementation at https://github.com/sykdomspulsen-org/scskeleton

We suggest that you clone this GitHub repo to your server, and then do a global find/replace on `scskeleton` with the name you want for your R package.

Descriptions of the required files/functions are detailed below.

## 00_env_and_namespace.r

https://github.com/sykdomspulsen-org/scskeleton/blob/main/R/00_env_and_namespace.r

```{r, echo=FALSE}
options(width = 150)
x <- fhiplot::as_github_code("https://raw.githubusercontent.com/sykdomspulsen-org/scskeleton/main/R/00_env_and_namespace.r")
print(x, include_url = TRUE)
```

## 01_definitions.r

https://github.com/sykdomspulsen-org/scskeleton/blob/main/R/01_definitions.r

```{r, echo=FALSE}
options(width = 150)
x <- fhiplot::as_github_code("https://raw.githubusercontent.com/sykdomspulsen-org/scskeleton/main/R/01_definitions.r")
print(x, include_url = TRUE)
```

## 02_permissions.r

https://github.com/sykdomspulsen-org/scskeleton/blob/main/R/02_permissions.r

```{r, echo=FALSE}
options(width = 150)
x <- fhiplot::as_github_code("https://raw.githubusercontent.com/sykdomspulsen-org/scskeleton/main/R/02_permissions.r")
print(x, include_url = TRUE)
```

## 03_db_schemas.r

https://github.com/sykdomspulsen-org/scskeleton/blob/main/R/03_db_schemas.r

```{r, echo=FALSE}
options(width = 150)
x <- fhiplot::as_github_code("https://raw.githubusercontent.com/sykdomspulsen-org/scskeleton/main/R/03_db_schemas.r")
print(x, include_url = TRUE)
```

## 04_tasks.r

https://github.com/sykdomspulsen-org/scskeleton/blob/main/R/04_tasks.r

```{r, echo=FALSE}
options(width = 150)
x <- fhiplot::as_github_code("https://raw.githubusercontent.com/sykdomspulsen-org/scskeleton/main/R/04_tasks.r")
print(x, include_url = TRUE)
```

## 05_deliverables.r

https://github.com/sykdomspulsen-org/scskeleton/blob/main/R/05_deliverables.r

```{r, echo=FALSE}
options(width = 150)
x <- fhiplot::as_github_code("https://raw.githubusercontent.com/sykdomspulsen-org/scskeleton/main/R/05_deliverables.r")
print(x, include_url = TRUE)
```

## 06_config.r

https://github.com/sykdomspulsen-org/scskeleton/blob/main/R/06_config.r

```{r, echo=FALSE}
options(width = 150)
x <- fhiplot::as_github_code("https://raw.githubusercontent.com/sykdomspulsen-org/scskeleton/main/R/06_config.r")
print(x, include_url = TRUE)
```

## 07_onLoad.r

https://github.com/sykdomspulsen-org/scskeleton/blob/main/R/07_onLoad.r

```{r, echo=FALSE}
options(width = 150)
x <- fhiplot::as_github_code("https://raw.githubusercontent.com/sykdomspulsen-org/scskeleton/main/R/07_onLoad.r")
print(x, include_url = TRUE)
```

## 08_onAttach.r

https://github.com/sykdomspulsen-org/scskeleton/blob/main/R/08_onAttach.r

```{r, echo=FALSE}
options(width = 150)
x <- fhiplot::as_github_code("https://raw.githubusercontent.com/sykdomspulsen-org/scskeleton/main/R/08_onAttach.r")
print(x, include_url = TRUE)
```

## 99_util_*.r

https://github.com/sykdomspulsen-org/scskeleton/blob/main/R/99_util_no_data_plot.r

```{r, echo=FALSE}
options(width = 150)
x <- fhiplot::as_github_code("https://github.com/sykdomspulsen-org/scskeleton/blob/main/R/99_util_no_data_plot.r")
print(x, include_url = TRUE)
```


## Task files

Task files are placed in .r files under their own names.

### weather_download_and_import_rawdata.r

https://github.com/sykdomspulsen-org/scskeleton/blob/main/R/weather_download_and_import_rawdata.r

```{r, echo=FALSE}
options(width = 150)
x <- fhiplot::as_github_code("https://raw.githubusercontent.com/sykdomspulsen-org/scskeleton/main/R/weather_download_and_import_rawdata.r")
print(x, include_url = TRUE)
```

### weather_clean_data.r

https://github.com/sykdomspulsen-org/scskeleton/blob/main/R/weather_clean_data.r

```{r, echo=FALSE}
options(width = 150)
x <- fhiplot::as_github_code("https://raw.githubusercontent.com/sykdomspulsen-org/scskeleton/main/R/weather_clean_data.r")
print(x, include_url = TRUE)
```

### weather_export_weather_plots.r

https://github.com/sykdomspulsen-org/scskeleton/blob/main/R/weather_export_plots.r

```{r, echo=FALSE}
options(width = 150)
x <- fhiplot::as_github_code("https://raw.githubusercontent.com/sykdomspulsen-org/scskeleton/main/R/weather_export_plots.r")
print(x, include_url = TRUE)
```
