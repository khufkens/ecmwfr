ads_service <- R6::R6Class("ecmwfr_ads", inherit = cds_service,
  public = list(
    submit = function() {
      if (private$status != "unsubmitted") {
        return(self)
      }
      # get key
      key <- wf_get_key(user = private$user, service = private$service)

      # fix strange difference in processing queries
      # from CDS
      body <- request
      body$dataset_short_name <- NULL
      body$target <- NULL
      response <- httr::POST(
        sprintf(
          "%s/resources/%s",
          url,
          request$dataset_short_name
        ),
        httr::authenticate(user, key),
        httr::add_headers("Accept" = "application/json",
                          "Content-Type" = "application/json"),
        body = body,
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

      private$status <- ct$state
      private$code <- ct$code
      private$name <- ct$request_id
      private$retry <- 5
      private$next_retry <- Sys.time() + private$retry
      private$url <- wf_server(id = ct$request_id, service = "ads")
      return(self)
    }
  ),
  private = list(
    service = "ads"
  )
)

