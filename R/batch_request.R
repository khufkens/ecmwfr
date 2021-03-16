wf_requests <- function(request_list,
                        workers = 2,
                        force_download = TRUE,
                        total_timeout = 3600*length(request_list)/workers) {
  slots <- as.list(rep(FALSE, workers))
  queue <- request_list
  done  <- list()

  timeout_time <- Sys.time() + total_timeout

  while (length(done) < length(request_list) & Sys.time() < timeout_time) {
    for (w in seq_along(slots)) {

      # If a slot is free and there's a queue,
      # asign to it the next pending request,
      # remove that request from the queue
      if (isFALSE(slots[[w]]) & length(queue) > 0) {
        slots[[w]] <- queue[[1]]
        queue[[1]] <- NULL
      }

      # Try to download
      if (!isFALSE(slots[[w]])) {
        slots[[w]]$download()
      }

      # If the slot is not still pending,
      # add the request to the "done" list
      # and free-up the slot
      if (!slots[[w]]$is_pending()) {
        done <- append(done, slots[[w]])
        slots[[w]] <- FALSE
      }

    }
  }

  unlist(lapply(done, function(x) x$get_file()))
}



