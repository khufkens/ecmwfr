cds_service <- R6::R6Class("ecmwfr_cds", inherit = service,
  public = list(
    submit = function() {
      if (private$status != "unsubmitted") {
        return(self)
      }
      # get key
      key <- wf_get_key(user = private$user, service = private$service)

      #  get the response for the query provided
      response <- httr::POST(
        sprintf(
          "%s/resources/%s",
        private$url,
          private$request$dataset_short_name
        ),
        httr::authenticate(private$user, key),
        httr::add_headers("Accept" = "application/json",
                          "Content-Type" = "application/json"),
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

      ct$code <- 202

      # some verbose feedback
      if (private$verbose) {
        message("- staging data transfer at url endpoint or request id:")
        message("  ", ct$request_id, "\n")
      }

      private$status <- "submitted"
      private$code <- ct$code
      private$name <- ct$request_id
      private$retry <- 5
      private$next_retry <- Sys.time() + private$retry
      private$url <- wf_server(id = ct$request_id, service = "cds")
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

      response <- httr::GET(
        private$url,
        httr::authenticate(private$user, key),
        httr::add_headers("Accept" = "application/json",
                          "Content-Type" = "application/json"),
        encode = "json"
      )

      ct <- httr::content(response)
      private$status <- ct$state


      if (private$status != "completed" || is.null(private$status)) {
        private$code <- 202
        private$file_url <- NA   # just ot be on the safe side
      }

      if (private$status == "completed") {
        private$code <- 302
        private$file_url <- ct$location
      } else if (private$status == "failed") {
        private$code <- 404
        permanent <- if (ct$error$permanent) "permanent "
        error_msg <- paste0("Data transfer failed with ", permanent, ct$error$who, " error: ",
                            ct$error$message, ".\nReason given: ", ct$error$reason, ".\n",
                            "More information at ", ct$error$url)
        warn_or_error(error_msg, error = fail_is_error)
      }
      private$next_retry <- Sys.time()
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
                            httr::write_disk(temp_file, overwrite = TRUE),
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
      url <- "https://cds.climate.copernicus.eu/user/login?destination=%2Fcdsapp%23!%2Fyourrequests"
      utils::browseURL(url)
      return(invisible(self))
    }
  ),
  private = list(
    service = "cds"
  )
)

