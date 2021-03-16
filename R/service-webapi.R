webapi_service <- R6::R6Class("ecmwfr_webapi", inherit = service,
  public = list(
    submit = function() {
      if (private$status != "unsubmitted") {
        return(self)
      }
      # get key
      key <- wf_get_key(user = private$user, service = private$service)

      #  get the response for the query provided
      response <- httr::POST(
        private$url,
        httr::add_headers(
          "Accept" = "application/json",
          "Content-Type" = "application/json",
          "From" = private$user,
          "X-ECMWF-KEY" = key
        ),
        body = private$request,
        encode = "json"
      )

      # trap general http error
      if (httr::http_error(response)) {
        stop(httr::content(response),
             call. = FALSE)
      }

      # grab content, to look at the status
      ct <- httr::content(response)

      # some verbose feedback
      if (private$verbose) {
        message("- staging data transfer at url endpoint or request id:")
        message("  ", ct$href, "\n")
      }

      # Return self invisibly.
      # TODO:
      #   - this used to return the content of the query, change it back?
      #   - replace exit_message with custom print function
      # if (!transfer) {
      #   message("  No download requests will be made, however...\n")
      #   exit_message(
      #     url = ct$href,
      #     path = path,
      #     file = request$target,
      #     service = service
      #   )
      #   return(invisible(ct))
      # }

      private$status <- ct$status
      private$code <- ct$code
      private$name <- ct$name
      private$retry <- as.numeric(ct$retry)
      private$next_retry <- Sys.time() + private$retry
      private$url <-  ct$href
      return(self)
    },

    update_status = function(fail_is_error = TRUE,
                             verbose = NULL) {
      if (private$status == "unsubmitted") {
        self$submit()
        return(self)
      }

      if (private$status == "deleted") {
        warn_or_error("Request was previously deleted from queue", call. = FALSE, error = fail_is_error)
        return(self)
      }

      if (private$status == "failed") {
        warn_or_error("Request has failed", call. = FALSE, error = fail_is_error)
        return(self)
      }

      key <- wf_get_key(user = private$user, service = private$service)

      retry_in <- as.numeric(private$next_retry) - as.numeric(Sys.time())

      if (retry_in > 0) {
        if (private$verbose) {
          # let a spinner spin for "retry" seconds
          spinner(retry_in)
        } else {
          # sleep
          Sys.sleep(retry_in)
        }
      }

      # Webapi
      response <- retrieve_header(private$url,
                                  list(
                                    "Accept" = "application/json",
                                    "Content-Type" = "application/json",
                                    "From" = private$user,
                                    "X-ECMWF-KEY" = key)
      )
      status_code <- response[["status_code"]]

      # If code = 200, then it's completed.
      if (status_code == "200") {
        private$status <-  "completed"
        private$code <- 302
        private$file_url <- response$url
        private$retry <- 0
        private$next_retry <- Sys.time() + private$retry
        return(self)
      }

      # Otherwise, need to get response and
      response <- response$get_response()
      ct <- httr::content(response)

      # Check for errors
      if (httr::http_error(response$status_code)) {
        private$status <- ct$status
        private$code <- status_code
        private$retry <- as.numeric(response$headers$`retry-after`)
        private$url <- ct$href
        private$next_retry <- Sys.time() + private$retry

        if (private$status == "rejected") {
          error_msg <- paste0("Your request was rejected. Reason given:\n", ct$reason)
        } else if (private$status == "aborted") {
          error_msg <- paste0("Your request was aborted. Reason given:\n", ct$reason)
        } else {
          error_msg <- paste0("Data transfer failed with error ",
                              ct$code, ".\nReason given: ", ct$reason, ".\n",
                              "More information at https://confluence.ecmwf.int/display/WEBAPI/Web+API+Troubleshooting")
        }
        warn_or_error(error_msg, error = fail_is_error)
        private$failed <- TRUE

        return(self)
      }

      # Is still processing.
      if (response$status_code == "202") {  # still processing
        # Simulated content with the things we need to use.
        private$status <- private$state <- "running"
        private$code <- status_code
        private$retry <- as.numeric(response$headers$`retry-after`)
        private$url <- response$headers$location
        private$next_retry <- Sys.time() + private$retry
      }
      private$next_retry <- Sys.time() + private$retry

      return(self)
    },

    download = function(force_redownload = FALSE, fail_is_error = TRUE, verbose = NULL) {

      # Check if download is actually needed
      if (private$downloaded == TRUE & file.exists(private$file) & !force_redownload) {
        if (private$verbose) message("File already downloaded")
        return(self)
      }

      # Check status
      self$update_status()

      if (private$status != "completed") {
        # if (private$verbose) message("\nRequest not completed")
        return(self)
      }

      # If it's completed, begin download
      if (private$verbose) message("\nDownloading file")
      temp_file <- tempfile(pattern = "ecmwfr_", tmpdir = private$path)
      key <- wf_get_key(user = private$user, service = private$service)

      response <- httr::GET(private$file_url,
                            httr::add_headers(
                              "Accept" = "application/json",
                              "Content-Type" = "application/json",
                              "From" = private$user,
                              "X-ECMWF-KEY" = key),
                            encode = "json",
                            httr::write_disk(temp_file, overwrite = TRUE),   # write on disk!
                            httr::progress())


      # trap (http) errors on download, return a general error statement
      if (httr::http_error(response)) {
        if (fail_is_error) {
          stop("Downlaod failed with error ", response$status_code)
        } else {
          warning("Downlaod failed with error ", response$status_code)
          return(self)
        }
      }

      private$downloaded <- TRUE
      # Copy data from temporary file to final location
      move <- suppressWarnings(file.rename(temp_file, private$file))

      # check if the move was successful
      # fails for separate disks/partitions
      # then copy and remove
      if (!move) {
        file.copy(temp_file, private$file, overwrite = TRUE)
        file.remove(temp_file)
      }

      if (private$verbose) {
        message(sprintf("- moved temporary file to -> %s", private$file))
      }

      return(self)
    },

    browse_request = function() {
      url <- paste0("https://apps.ecmwf.int/webmars/joblist/", private$name)
      utils::browseURL(url)
      return(invisible(self))
    }
  ),
  private = list(
    service = "webapi"
  )
)

