service <- R6::R6Class("ecmwfr_service", cloneable = FALSE,
  public = list(
    initialize = function(request,
                          user,
                          url,
                          path = tempdir(),
                          verbose = TRUE) {
      private$user <- user
      private$request <- request
      private$path <- path
      private$file <- file.path(path, request$target)
      private$verbose <- verbose
      private$url <- url
      private$status <- "unsubmitted"
      private$next_retry <- Sys.time()

      return(self)
    },

    print = function(...) {
      request <- capture.output(str(private$request, 1))
      cat("Download request \n")
      cat("  Service: ", private$service, "\n")
      cat("  Status:  ", private$status, "\n")
      cat("  Location:", if (private$downloaded) private$file else "NA", "\n")
      cat("  Request:", request, sep = "\n     ")

      invisible(self)
    },

    submit = function() {
     stop("not implemented")
    },

    update_status = function(fail_is_error = TRUE,
                             verbose = NULL) {
      stop("not implemented")
      },

    download = function(force_redownload = FALSE, fail_is_error = TRUE, verbose = NULL) {
     stop("not implemented")
    },

    transfer = function(time_out = 3600, verbose = NULL) {
      if (private$verbose) {
        message(sprintf("- timeout set to %.1f hours", time_out/3600))
      }

      # set time-out
      time_out <- Sys.time() + time_out

      # Try to download while pending.
      # Quit if timed out
      while (Sys.time() < time_out) {
        self$download()

        if (private$downloaded) {
          break
        }
      }

      if (self$is_pending()) {
        if (private$verbose) {
          # needs to change!
          message("  Your download timed out, however ...\n")
          # self$exit_message()  # TODO
        }
      }
      return(self)

    },

    browse_request = function() {
      stop("not implemented")
    },

    get_file = function() {
      if (private$downloaded) {
        return(private$file)
      } else {
        return(NA)
      }
    },

    get_status = function() {
      private$status
    },

    get_request = function() {
      private$request
    },

    is_failed = function() {
      private$failed
    },

    is_success = function() {
      private$downloaded
    },

    is_running = function() {
      private$status == "running"
      # !(self$code == 302)
    },

    is_pending = function() {
      # Always pending, unless it has failed or successfully downloaded.
      !(self$is_failed() | private$downloaded)
    }

  ),
  private = list(
    service = NA,
    request = NA,

    user = NA,

    path = NA,
    file = NA,
    time_out = NA,

    status = NA,
    failed = FALSE,
    code = NA,
    name = NA,
    retry = NA,
    next_retry = NA,
    url = NA,
    file_url = NA,

    verbose = TRUE,
    downloaded = FALSE

  )
)

