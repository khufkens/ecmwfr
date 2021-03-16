#' ECMWF data request and download
#'
#' Stage a data request, and optionally download the data to disk. Alternatively
#' you can only stage requests, logging the request URLs to submit download
#' queries later on using \code{\link[ecmwfr]{wf_transfer}}.
#' Note that the function will do some basic checks on the \code{request} input
#' to identify possible problems.
#'
#' @param user user (email address) used to sign up for the ECMWF data service,
#' used to retrieve the token set by \code{\link[ecmwfr]{wf_set_key}}
#' @param path path were to store the downloaded data
#' @param time_out how long to wait on a download to start (default =
#' \code{3*3600} seconds).
#' @param transfer logical, download data TRUE or FALSE (default = TRUE)
#' @param request nested list with query parameters following the layout
#' as specified on the ECMWF APIs page
#' @param job_name optional name to use as an RStudio job and as output variable
#'  name. It has to be a syntactically valid name.
#' @param verbose show feedback on processing

#' @return a download query staging url or (invisible) filename of the file on
#' your local disc
#' @seealso \code{\link[ecmwfr]{wf_set_key}}
#' \code{\link[ecmwfr]{wf_transfer}}
#' @export
#' @author Koen Hufkens
#' @examples
#'
#' \dontrun{
#' # set key
#' wf_set_key(user = "test@mail.com", key = "123")
#'
#' request <- list(stream = "oper",
#'    levtype = "sfc",
#'    param = "167.128",
#'    dataset = "interim",
#'    step = "0",
#'    grid = "0.75/0.75",
#'    time = "00",
#'    date = "2014-07-01/to/2014-07-02",
#'    type = "an",
#'    class = "ei",
#'    area = "50/10/51/11",
#'    format = "netcdf",
#'    target = "tmp.nc")
#'
#' # demo query
#' wf_request(request = request, user = "test@mail.com")
#'
#' # Run as an RStudio Job. When finished, will create a
#' # variable named "test" in your environment with the path to
#' # the downloaded file.
#' wf_request(request = request, user = "test@mail.com", job_name = "test")
#'}

wf_request <- function(request,
                       user,
                       transfer = TRUE,
                       path = tempdir(),
                       time_out = 3600,
                       job_name,
                       verbose = TRUE) {
  if (!missing(job_name)) {
    if (make.names(job_name) != job_name) {
      stop("job_name '",
           job_name,
           "' is not a syntactically valid variable name.")
    }

    # Evaluates all arguments.
    call <- match.call()
    call$path <- path
    call_list <- lapply(call, eval)
    call[names(call_list)[-1]] <- call_list[-1]

    script <- make_script(call = call, name = job_name)
    if (!requireNamespace("rstudioapi", quietly = TRUE)) {
      stop("Jobs are only supported in RStudio.")
    }

    if (!rstudioapi::isAvailable("1.2")) {
      stop(
        "Need at least version 1.2 of RStudio to use jobs. Currently running ",
        rstudioapi::versionInfo()$version,
        "."
      )
    }

    job <-
      rstudioapi::jobRunScript(path = script,
                               name = job_name,
                               exportEnv = "R_GlobalEnv")
    return(invisible(job))
  }

  if (!is.list(request) | is.character(request)) {
    stop(
      "`request` must be a named list. \n",
      "If you are passing the user as first argument, notice that argument ",
      "order was changed in version 1.1.1."
    )
  }

  # check the login credentials
  if (missing(request)) {
    stop("Please provide ECMWF or CDS login credentials and data request!")
  }

  if (missing(user)) {
    user <-
      rbind(
        keyring::key_list(service = make_key_service(c("webapi"))),
        keyring::key_list(service = make_key_service(c("cds"))),
        keyring::key_list(service = make_key_service(c("ads")))
      )
    serv <- make_key_service()
    user <-
      user[substr(user$service, 1,  nchar(serv)) == serv, ][["username"]]
  }

  # checks user login, the request layout and
  # returns the service to use if successful
  wf_check <-
    lapply(user, function(u)
      try(wf_check_request(u, request), silent = TRUE))
  correct <- which(!vapply(wf_check, inherits, TRUE, "try-error"))

  if (length(correct) == 0) {
    stop(
      sprintf(
        "Data identifier %s is not found in Web API, CDS or ADS datasets.
                 Or your login credentials do not match your request.",
        request$dataset_short_name
      ),
      call. = FALSE
    )
  }

  wf_check <- wf_check[[correct]]
  user <- user[correct]

  if (verbose)
  {
    message("Requesting data to the ",
            wf_check$service,
            " service with username ",
            user)
  }

  # split out data
  service <- wf_check$service
  url <- wf_check$url


  service <- switch(service,
                    webapi = webapi_service,
                    cds = cds_service,
                    ads = ads_service)


  request <- service$new(request = request,
                     user = user,
                     url = url,
                     path = path)
  if (transfer) {
    request$transfer(time_out = time_out)
    if (request$is_success()) {
      return(request$get_file())
    }
    warning("Transfer was not successfull, returning request object")
  }

  return(request)
}
