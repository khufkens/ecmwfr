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
#' as specified on the ECMWF API page
#' @param job_name optional name to use as an RStudio job and as output variable
#'  name. It has to be a syntactically valid name.
#' @param verbose show feedback on processing

#' @return a download query staging url or (invisible) filename of the file on
#' your local disc
#' @keywords data download, climate, re-analysis
#' @seealso \code{\link[ecmwfr]{wf_set_key}}
#' \code{\link[ecmwfr]{wf_transfer}}
#' @export
#' @author Koen Kufkens
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

wf_request <- function(
  request,
  user,
  transfer = TRUE,
  path = tempdir(),
  time_out = 3600,
  job_name,
  verbose = TRUE
){

  if (!missing(job_name)) {

    if (make.names(job_name) != job_name) {
      stop("job_name '", job_name, "' is not a syntactically valid variable name.")
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
      stop("Need at least version 1.2 of RStudio to use jobs. Currently running ",
           rstudioapi::versionInfo()$version, ".")
    }

    job <- rstudioapi::jobRunScript(path = script, name = job_name, exportEnv = "R_GlobalEnv")
    return(invisible(job))
  }

  new_transfer <- transfer_obj$new(request = request, user = user, path = path,
                               time_out = time_out, verbose = verbose)

  # only return the content of the query
  if(!transfer){
    if (verbose) message("  No download requests will be made, however...\n")
    new_transfer$exit_message()
    return(invisible(new_transfer))
  }

  new_transfer$transfer()

  new_transfer$delete()

  # return final file name/path (dst = destination).
  return(invisible(new_transfer$file))
}


