# Returns server URL
#
# Returns the url of the data servers for downloading
# public ECMWF data sets or data sets for the Copernicus CDS.
#
# @param id string, id of the request.
# @details The handling of CDS requests is slightly different from
# the one of ECMWF public data sets. To be able to get the status
# of a request for CDS data sets we do have to check the status
# by calling the 'task' urls. If an \code{id} is given the
# corresponding 'task' url will be returned. Used in
# \code{\link[ecmwfr]{cds_request} and \code{\link[ecmwfr]{cds_transfer}}
# (to be more explicit: in \code{\link[ecmwfr]{wf_transfer} if
# \code{type == "cds"}).
#
# @author Koen Kufkens
wf_server <- function(id, service = "webapi") {

  # match arguments, if not stop
  service <- match.arg(service, c("webapi", "cds"))

  # set base urls
  webapi_url <- "https://api.ecmwf.int/v1"
  cds_url <- "https://cds.climate.copernicus.eu/api/v2"

  # return url depending on service or id
  if (service == "webapi"){
    if(missing(id)){
      return(webapi_url)
    } else {
      return(file.path(webapi_url, "services/mars/requests", id))
    }
  } else {
    if(missing(id)){
      return(cds_url)
    } else {
      return(file.path(cds_url, "tasks", id))
    }
  }
}

# Simple progress spinner
#
# Shows a spinner while waiting for a request to be processed.
#
# @param seconds integer, seconds to sleep
#
# @details Shows a spinner while waiting for a request to be processed.
# If \code{id} (character) is set, the request id will be shown in addition.
#
# @author Koen Kufkens, Reto Stauffer
spinner <- function(seconds){

  # set start time, counter
  start_time <- Sys.time()
  spinner_count <- 1

  while(Sys.time() <= start_time + seconds){

    # slow down while loop
    Sys.sleep(0.2)

    # update spinner message
    message(paste0(c("-","\\","|","/")[spinner_count],
                   " polling server for a data transfer\r"),
            appendLF = FALSE)

    # update spinner count
    spinner_count <- ifelse(spinner_count < 4, spinner_count + 1, 1)
  }
}

# Show message if user exits the function (interrupts execution)
# or as soon as an error will be thrown.
exit_message <- function(url, service, path, file){

  job_list <- ifelse(service == "webapi",
    "  Visit https://apps.ecmwf.int/webmars/joblist/",
    "  Visit https://cds.climate.copernicus.eu/cdsapp#!/yourrequests")

  intro <- paste(
    "Even after exiting your request is still beeing processed!",
    job_list,
    "  to manage (download, retry, delete) your requests",
    "  or to get ID's from previous requests.\n\n", sep = "\n")

  options <- paste(
    "- Retry downloading as soon as as completed:\n",
    "  wf_transfer(url = '",url, "\n",
    "<user>,\n ",
    "',\n path = '",path,
    "',\n filename = '",file,
    "',\n service = \'", service,"')\n\n",
    "- Delete the job upon completion using:\n",
    "  wf_delete(<user>,\n url ='",url,"')\n\n",
    sep = "")

  # combine all messages
  exit_msg <- paste(intro, options, sep = "")
  message(sprintf("- Your request has been submitted as a %s request.\n\n  %s",
                  toupper(service),exit_msg))
}

# Startup message when attaching the package.
.onAttach <- function(libname = find.package("ecmwfr"), pkgname = "ecmwfr") {
  vers <- as.character(utils::packageVersion("ecmwfr"))
  txt <- paste("\n     This is 'ecmwfr' version ",
               vers,". Please respect the terms of use:\n",
               "     - https://cds.climate.copernicus.eu/disclaimer-privacy\n",
               "     - https://www.ecmwf.int/en/terms-use\n")
  if(interactive()) packageStartupMessage(txt)
}

# check if server is reachable
# returns bolean TRUE if so
ecmwf_running <- function(url){
  ct <- try(httr::GET(url))

  # trap time-out, httr should exit clean but doesn't
  # it seems
  if (inherits(ct, "try-error")){
    return(FALSE)
  }

  # trap 400 errors
  if(ct$status_code >= 404 ){
    return(FALSE)
  } else {
    return(TRUE)
  }
}

# builds keychain service name from service
make_key_service <- function(service = "") {
  paste("ecmwfr", service, sep = "_")
}

# gets url where to get API key
wf_key_page <- function(service) {
  switch(service,
         webapi = "https://api.ecmwf.int/v1/key/",
         cds = "https://cds.climate.copernicus.eu/user/login?destination=user")
}

# checks credentials
wf_check_login <- function(user, key, service) {
  if (service == "webapi") {
    info <- httr::GET(
      paste0(wf_server(),
             "/who-am-i"),
      httr::add_headers(
        "Accept" = "application/json",
        "Content-Type" = "application/json",
        "From" = user,
        "X-ECMWF-KEY" = key),
      encode = "json"
    )
    return(!httr::http_error(info) && (httr::content(info)$uid == user))
  }

  if (service == "cds") {
    url <- paste0(wf_server(service = "cds"),"/tasks/")
    ct <- httr::GET(url, httr::authenticate(user, key))
    return(httr::status_code(ct) < 400)
  }
}

# build an archetype from arguments and body (either list or expression)
new_archetype <- function(args, body) {
  if (is.list(body)) {
    body_exp <- rlang::expr(list())
    body_exp[names(body)] <- body
    body <- body_exp
  }
  f <- rlang::new_function(args, body)
  class(f) <- c("ecmwfr_archetype", class(f))
  f
}


# Creates a script to then run as a job
make_script <- function(call, name) {
  script <- tempfile()

  call$job_name <- NULL

  lines <- writeLines(paste0("library(ecmwfr)\n", name, " <- ", paste0(deparse(call), collapse = "")), script)
  return(script)
}


# Downlaods only the header information
retrieve_header <- function(url, headers) {
  h <- curl::new_handle()
  curl::handle_setheaders(h, .list = headers)
  con <- curl::curl(url, handle = h)

  open(con, "rf")
  head <- curl::handle_data(h)
  close(con)

  head$headers <- curl::parse_headers_list(head$headers)
  return(list(header = head,
              get_response = function() {
                httr::GET(url, do.call(httr::add_headers, headers))
              }))
}


# Encapsulates errors are warnings logic.
warn_or_error <- function(..., error = FALSE) {
  if (error) {
    stop(...)
  } else {
    warning(...)
  }
}
