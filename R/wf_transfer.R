#' ECMWF data transfer function
#'
#' Returns the contents of the requested url as a NetCDF file downloaded
#' to disk or the current status of the requested transfer.
#'
#' @param user user (email address) used to sign up for the ECMWF data service,
#' used to retrieve the token set by \code{\link[ecmwfr]{wf_set_key}}.
#' @param url url to query
#' @param service which service to use, one of \code{webapi}, \code{cds}
#' or \code{ads} (default = webapi)
#' @param path path were to store the downloaded data
#' @param filename filename to use for the downloaded data
#' @param verbose show feedback on data transfers
#' @return a netCDF of data on disk as specified by a
#' \code{\link[ecmwfr]{wf_request}}
#' @seealso \code{\link[ecmwfr]{wf_set_key}}
#' \code{\link[ecmwfr]{wf_request}}
#' @export
#' @author Koen Hufkens
#' @examples
#'
#' \dontrun{
#' # set key
#' wf_set_key(user = "test@mail.com", key = "123")
#'
#' # request data and grab url and try a transfer
#' r <- wf_request(request, "test@email.com", transfer = FALSE)
#'
#' # check transfer, will download if available
#' wf_transfer(r$href, "test@email.com")
#'}

wf_transfer <- function(url,
                        user,
                        service = "webapi",
                        path = tempdir(),
                        filename = tempfile("ecmwfr_"),
                        verbose = TRUE) {

  if (inherits(url, "ecmwfr_service")) {
    url$transfer()
    return(url)
  }

  # match arguments, if not stop
  service <- match.arg(service, c("webapi", "cds", "ads"))

  # check the login credentials
  if (missing(user) || missing(url)) {
    stop("Please provide ECMWF login email / url!")
  }

  # If the URL is not an URL but an ID: generate URL
  if (service == "cds" | service == "ads") {
    url <- wf_server(id = url, service = service)
  }

  # get key
  key <- wf_get_key(user = user, service = service)

  # create (temporary) output file
  tmp_file <- file.path(path, filename)

  # download routine depends on service queried
  if (service == "cds" | service == "ads") {
    response <- httr::GET(
      url,
      httr::authenticate(user, key),
      httr::add_headers("Accept" = "application/json",
                        "Content-Type" = "application/json"),
      encode = "json"
    )
  } else {
    # Webapi
    response <- retrieve_header(
      url,
      list(
        "Accept" = "application/json",
        "Content-Type" = "application/json",
        "From" = user,
        "X-ECMWF-KEY" = key
      )
    )
    status_code <- response[["status_code"]]

    if (httr::http_error(status_code)) {
      stop("Your requested download failed - check url", call. = FALSE)
    }

    if (status_code == "202") {
      # still processing
      # Simulated content with the things we need to use.
      ct <- list(
        code = status_code,
        retry = as.numeric(response$headers$`retry-after`),
        href = url
      )
      return(invisible(ct))

    } else if (status_code == "200") {
      # Done!
      message("\nDownloading file")
      response <- httr::GET(
        url,
        httr::add_headers(
          "Accept" = "application/json",
          "Content-Type" = "application/json",
          "From" = user,
          "X-ECMWF-KEY" = key
        ),
        encode = "json",
        httr::write_disk(tmp_file, overwrite = TRUE),
        # write on disk!
        httr::progress()
      )

      return(invisible(list(code = 302,
                            href = url)))
    } else {
      stop("Your requested download had a problem with code ",
           status_code,
           call. = FALSE)
    }
  }

  # trap (http) errors on download, return a general error statement
  if (httr::http_error(response)) {
    stop("Your requested download failed - check url", call. = FALSE)
  }

  # check the content, and status of the download
  # will fail on large (binary) files
  ct <- httr::content(response)

  # write raw data to file from memory
  # if not returned url + passing code
  if (inherits(ct, "raw") && service == "webapi") {
    if (verbose) {
      message("- polling server for a data transfer")
      message(sprintf("- writing data to disk (\"%s\")", tmp_file))
    }

    # write binary file
    f <- file(tmp_file, "wb")
    writeBin(ct, f)
    close(f)

    # return data
    return(invisible(list(code = 302,
                          href = url)))
  }

  if (service == "cds" | service == "ads") {

    # if the transfer failed, return error and stop()
    if (ct$state == "failed") {
      message("Data transfer failed!")
      stop(ct$error)
    }

    if (ct$state != "completed" || is.null(ct$state)) {
      ct$code <- 202
    }

    # if completed / should not happen but still there
    if ("completed" == ct$state) {
      # download file
      httr::GET(ct$location,
                httr::write_disk(tmp_file, overwrite = TRUE),
                httr::progress())

      # return exit statement
      ct$code <- 302
    }
  }

  # return state variable
  return(invisible(ct))
}
