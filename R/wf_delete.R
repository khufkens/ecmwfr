#' ECMWF delete request
#'
#' Deletes a staged download from the queue
#'
#' @param user user (email address) used to sign up for the ECMWF data service,
#' used to retrieve the token set by \code{\link[ecmwfr]{wf_set_key}}
#' @param url url to query
#' @param verbose show feedback on processing
#' @param service character, one of \code{ecmwf} or \code{cds} depending
#' on the data set to be deleted.
#' @keywords data download, climate, re-analysis
#' @seealso \code{\link[ecmwfr]{wf_set_key}}
#' \code{\link[ecmwfr]{wf_transfer}}
#' \code{\link[ecmwfr]{wf_request}}
#' @export
#' @author Koen Kufkens
#' @examples
#'
#' \dontrun{
#' # set key
#' wf_set_key(email = "test@mail.com", key = "123")
#'
#' # get key
#' wf_get_key(email = "test@mail.com")
#'}

wf_delete <- function(
  url,
  user,
  service = "webapi",
  verbose = TRUE
){

  if (inherits(url, "wf_transfer")) {
    url$delete(verbose = verbose)
    return(invisible(url))
  }
  # check the login credentials
  if(missing(user) | missing(url)){
    stop("Please provide ECMWF login user / url!")
  }

  # match arguments, if not stop
  service <- match.arg(service, c("webapi", "cds"))

  # get key
  key <- wf_get_key(user = user, service = service)

  # If the URL is not an URL but an ID: generate URL
  if (service == "cds") {
    url <- wf_server(id = url, service = service)
  }

  # remove a queued download
  # Differs for ecmwf and cds requests.
  # For CDS: note that 'user' is simply a copy of 'user'
  if(service == "cds") {
    response <- httr::DELETE(
      url,
      httr::authenticate(user, key),
      httr::add_headers(
        "Accept" = "application/json",
        "Content-Type" = "application/json")
    )
  } else {
    response <- httr::DELETE(
      url,
      httr::add_headers(
        "Accept" = "application/json",
        "Content-Type" = "application/json",
        "From" = user,
        "X-ECMWF-KEY" = key)
    )
  }

  # check purging of request
  if(response$status == 204){
    if (verbose){
      message("- request purged from queue!")
    }
  } else {
    warning("Request not purged from queue, check download!")
    return(invisible(FALSE))
  }
  return(invisible(TRUE))
}
