transfer_obj <- R6::R6Class("wf_transfer",
  public = list(
    initialize = function(request,
                          user,
                          path = tempdir(),
                          time_out = 3600,
                          verbose = TRUE) {
      if(!is.list(request) | is.character(request)) {
        stop("`request` must be a named list. \n",
             "If you are passing the user as first argument, notice that argument ",
             "order was changed in version 1.1.1.")
      }

      # check the login credentials
      if(missing(request)){
        stop("Please provide ECMWF or CDS login credentials and data request!")
      }

      if (missing(user)) {
        user <- rbind(keyring::key_list(service = make_key_service(c("webapi"))),
                      keyring::key_list(service = make_key_service(c("cds"))))
        serv <- make_key_service()
        user <- user[substr(user$service, 1,  nchar(serv)) == serv, ][["username"]]
      }

      # checks user login, the request layout and
      # returns the service to use if successful
      wf_check <- lapply(user, function(u) try(wf_check_request(u, request), silent = TRUE))
      correct <- which(!vapply(wf_check, inherits, TRUE, "try-error"))
      if (length(correct) == 0) {
        stop("Invalid request")
      }
      wf_check <- wf_check[[correct]]
      user <- user[correct]
      self$user <- user

      if (verbose) {
        message("Requesting data to the ", wf_check$service, " service with username ", user)
      }


      if (length(wf_check) == 0) {
        stop(sprintf("Data identifier %s is not found in Web API or CDS datasets.
                 Or your login credentials do not match your request.",
                     request$dataset), call. = FALSE)
      }

      # split out data
      service <- wf_check$service
      self$service <- service
      url <- wf_check$url
      self$url <- self$href <- url

      # get key
      key <- wf_get_key(user = user, service = service)

      # getting api url: different handling if 'dataset = "mars"',
      # requests to 'dataset = "mars"' require a non-public user
      # account (member states/commercial).

      # depending on the service get the response
      # for the query provided
      if (service == "webapi"){
        response <- httr::POST(
          url,
          httr::add_headers(
            "Accept" = "application/json",
            "Content-Type" = "application/json",
            "From" = user,
            "X-ECMWF-KEY" = key),
          body = request,
          encode = "json"
        )
      } else {
        response <- httr::POST(
          sprintf("%s/resources/%s", wf_server(service = "cds"),
                  request$dataset),
          httr::authenticate(user, key),
          httr::add_headers(
            "Accept" = "application/json",
            "Content-Type" = "application/json"),
          body = request,
          encode = "json"
        )
      }

      # trap general http error
      if(httr::http_error(response)){
        stop(httr::content(response),
             call. = FALSE)
      }

      # grab content, to look at the status
      ct <- httr::content(response)

      self$request <- request
      self$path <- path
      self$file <- file.path(path, request$target)
      self$time_out <- time_out
      self$verbose <- verbose

      # first run is always 202
      if (service == "cds"){
        self$status <- self$state <- ct$state
        self$code <- 202
        self$name <- self$request_id <- ct$request_id
        self$retry <- 5
        self$next_retry <- Sys.time() + self$retry
        self$url <- self$href <-  wf_server(id = ct$request_id, service = service)
      } else if (service == "webapi") {
        self$status <- self$state <- ct$status
        self$code <- ct$code
        self$name <- self$request_id <- ct$name
        self$retry <- as.numeric(ct$retry)
        self$next_retry <- Sys.time() + self$retry
        self$url <- self$href <-  ct$href
      }

      # some verbose feedback
      if(verbose){
        message("- staging data transfer at url endpoint or request id:")
        message("  ", ifelse(service == "cds", ct$request_id, ct$href), "\n")
      }

      return(invisible(self))
    },

    update_status = function(fail_is_error = FALSE, verbose = NULL) {
      self$set_verbose(verbose)

      if (self$status == "deleted") {
        warn_or_error("Request was previously deleted from queue", call. = FALSE, error = fail_is_error)
        return(invisible(self))
      }

      key <- wf_get_key(user = self$user, service = self$service)
      retry_in <- as.numeric(self$next_retry) - as.numeric(Sys.time())
      if (retry_in > 0) {
        if(self$verbose) {
          # let a spinner spin for "retry" seconds
          spinner(retry_in)
        } else {
          # sleep
          Sys.sleep(retry_in)
        }
      }

      if(self$service == "cds") {
        response <- httr::GET(self$url,
                              httr::authenticate(self$user, key),
                              httr::add_headers(
                                "Accept" = "application/json",
                                "Content-Type" = "application/json"),
                              encode = "json")

        if (httr::http_error(response)) {
          warn_or_error("Your requested download failed - check url", call. = FALSE, error = fail_is_error)
        }

        ct <- httr::content(response)
        self$status <- ct$state
        self$state <- self$status

        if (self$status != "completed" || is.null(self$status)) {
          self$code <- 202
          self$file_url <- NA   # just ot be on the safe side
        }

        if (self$status == "completed") {
          self$code <- 302
          self$file_url <- ct$location
        } else if (self$status == "failed") {
          permanent <- if (ct$error$permanent) "permanent "
          error_msg <- paste0("Data transfer failed with ", permanent, ct$error$who, " error: ",
                              ct$error$message, ".\nReason given: ", ct$error$reason, ".\n",
                              "More information at ", ct$error$url)
          warn_or_error(error_msg, error = fail_is_error)
        }
        self$next_retry <- Sys.time() + self$retry
        return(invisible(self))


      } else {
        # Webapi
        response <- retrieve_header(self$url,
                                    list(
                                      "Accept" = "application/json",
                                      "Content-Type" = "application/json",
                                      "From" = self$user,
                                      "X-ECMWF-KEY" = key)
        )

        status_code <- response$header[["status_code"]]
        if (status_code == "200") {  # Done!
            self$status <- self$state <-  "completed"
            self$code <- 302
            self$url <- self$href <-  response$header$url
            self$file_url <- response$header$url
            self$next_retry <- Sys.time() + self$retry
            return(invisible(self))
        }

        response <- response$get_response()

        if (httr::http_error(response$status_code)) {
          ct <- httr::content(response)
          self$status <- self$state <- ct$status
          self$code <- status_code
          self$retry <- as.numeric(ct$retry)
          self$url <- self$href <- ct$href
          self$next_retry <- Sys.time() + self$retry
          if (self$status == "rejected") {
            error_msg <- paste0("Your request was rejected. Reason given:\n", ct$reason)
          } else if (self$status == "aborted") {
            error_msg <- paste0("Your request was aborted. Reason given:\n", ct$reason)
          } else {
            error_msg <- paste0("Data transfer failed with error ",
                                ct$code, ".\nReason given: ", ct$reason, ".\n",
                                "More information at https://confluence.ecmwf.int/display/WEBAPI/Web+API+Troubleshooting")
          }
          warn_or_error(error_msg, error = fail_is_error)
          return(invisible(self))
        }

        if (status_code == "202") {  # still processing
          # Simulated content with the things we need to use.
          self$status <- self$state <- "running"
          self$code <- status_code
          self$retry <- as.numeric(response$header$headers$`retry-after`)
          self$url <- self$href <-  response$header$url
          self$next_retry <- Sys.time() + self$retry
        } else {
          self$next_retry <- Sys.time() + self$retry
          warn_or_error("Data transfer failed with error ", response$header$status_code, error = fail_is_error)

        }
        return(invisible(self))
      }
    },

    is_running = function() {
      !(self$code == 302)
    },

    is_pending = function() {
      !(self$status == "failed" | self$downloaded)
    },

    open_request = function() {
      if (self$service == "webapi") {
        url <- paste0("https://apps.ecmwf.int/webmars/joblist/", self$name)
      } else if (self$service == "cds") {
        url <- paste0("https://cds.climate.copernicus.eu/user/login?destination=%2Fcdsapp%23!%2Fyourrequests")
      }
      utils::browseURL(url)
      return(invisible(self))
    },

    download = function(force_redownload = FALSE, fail_is_error = FALSE, verbose = NULL) {
      self$set_verbose(verbose)
      if (self$downloaded == TRUE & file.exists(self$file) & !force_redownload) {
        if (self$verbose) message("File already downloaded")
        return(invisible(self))
      }

      self$update_status()

      if (self$status == "completed") {
        if (self$verbose) message("\nDownloading file")
        temp_file <- tempfile(pattern = "ecmwfr_", tmpdir = self$path)
        if (self$service == "cds") {
          response <- httr::GET(self$file_url,
                                httr::write_disk(temp_file, overwrite = TRUE),
                                httr::progress())
        } else {
          # webapi
          key <- wf_get_key(user = self$user, service = self$service)
          response <- httr::GET(self$file_url,
                                httr::add_headers(
                                  "Accept" = "application/json",
                                  "Content-Type" = "application/json",
                                  "From" = self$user,
                                  "X-ECMWF-KEY" = key),
                                encode = "json",
                                httr::write_disk(temp_file, overwrite = TRUE),   # write on disk!
                                httr::progress())
        }

        if (httr::http_error(response)) {
          if (fail_is_error) {
            stop("Downlaod failed with error ", response$status_code)
          } else {
            warning("Downlaod failed with error ", response$status_code)
          }
        }

        move <- suppressWarnings(file.rename(temp_file, self$file))

        if(!move){
          file.copy(temp_file, self$file, overwrite = TRUE)
          file.remove(temp_file)
        }

        if (self$verbose){
          message(sprintf("- moved temporary file to -> %s", self$file))
        }

        self$downloaded <- TRUE
      } else {
        if (self$verbose) message("\nRequest not completed")
      }
      return(invisible(self))
    },

    delete = function(verbose = NULL) {
      self$set_verbose(verbose)

      # delete the request upon succesful download
      # to free up other download slots. Not possible
      # for ECMWF mars requests (skip)
      if(!self$request$dataset == "mars") {
        deleted <- wf_delete(user    = self$user,
                  url     = if (self$service == "cds") self$name else self$url,
                  verbose = self$verbose,
                  service = self$service)
        if (deleted) {
          self$status <- self$state <- "deleted"
          self$code <- 404
        }
      } else {
        warning("Deleting request is not possible for MARS requests")
      }
      return(invisible(self))
    },

    transfer = function(verbose = NULL) {
      self$set_verbose(verbose)

      if(self$verbose){
        message(sprintf("- timeout set to %.1f hours", self$time_out/3600))
      }

      # set time-out
      time_out <- Sys.time() + self$time_out
      self$update_status()
      while(self$code == 202) { # check formatting state variable CDS
        # exit routine when the time out
        if (Sys.time() > time_out) {
          if (self$verbose) {
            # needs to change!
            message("  Your download timed out, however ...\n")
            self$exit_message()
          }
          return(invisible(self))
        }

        self$download()
      }
      return(invisible(self))

    },
    set_verbose = function(verbose = TRUE) {
      if (!is.null(verbose)) {
        self$verbose <- verbose
      }
      return(invisible(self))
    },

    exit_message = function(){
      job_list <- ifelse(self$service == "webapi",
                         "  Visit https://apps.ecmwf.int/webmars/joblist/",
                         "  Visit https://cds.climate.copernicus.eu/cdsapp#!/yourrequests")
      url <- if (self$service == "cds") self$name else self$url
      intro <- paste(
        "Even after exiting your request is still beeing processed!",
        job_list,
        "  to manage (download, retry, delete) your requests",
        "  or to get ID's from previous requests.\n\n", sep = "\n")

      options <- paste(
        "- Retry downloading as soon as as completed:\n",
        "  wf_transfer(url = '", url,
        "',\n user ='", self$user,
        "',\n path = '",self$path,
        "',\n filename = '", basename(self$file),
        "',\n service = \'", self$service,"')\n\n",
        "- Delete the job upon completion using:\n",
        "  wf_delete('", self$user,
        ",'\n url ='", url,"')\n\n",
        sep = "")

      # combine all messages
      exit_msg <- paste(intro, options, sep = "")
      message(sprintf("- Your request has been submitted as a %s request.\n\n  %s",
                      toupper(self$service), exit_msg))
    },
    request = NA,
    path = NA,
    file = NA,
    downloaded = FALSE,
    time_out = NA,

    user = NA,
    service = NA,

    #  Conflicting names for webapi vs CDS variables
    #  Will use webapi names and leave the other for
    #  backward compatibilty
    status = NA,     # webapi
    state = NA,      # CDS

    name = NA,       # webapi
    request_id = NA, # CDS


    url = NA,
    href = NA,

    code = 404,


    retry = NA,
    next_retry = NA,

    file_url = NA,

    verbose = TRUE

  )
)
