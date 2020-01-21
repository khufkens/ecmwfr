# set options
opts <- options(keyring_warn_for_env_fallback = FALSE)
on.exit(options(opts), add = TRUE)
user <- "2088"

# format request (see below)
cds_request <- list(
              "dataset"        = "reanalysis-era5-pressure-levels",
              "product_type"   = "reanalysis",
              "format"         = "netcdf",
              "variable"       = "temperature",
              "pressure_level" = "850",
              "year"           = "2018",
              "month"          = "04",
              "day"            = "04",
              "time"           = "00:00",
              "area"           = "50/9/51/10",
              "format"         = "netcdf",
              "target"         = "era5-demo.nc")

# is the server reachable
server_check <- !ecmwf_running(wf_server(service = "cds"))

# if the server is reachable, try to set login
# if not set login check to TRUE as well
if(!server_check){
  key <- system("echo $CDS", intern = TRUE)
  if(key != "" & key != "$CDS"){
    wf_set_key(user = user,
               key = key,
               service = "cds")
  }
  rm(key)

  login_check <- try(wf_get_key(user = user,
                                service = "cds"), silent = TRUE)
  login_check <- inherits(login_check, "try-error")
  server_check <- !ecmwf_running(wf_server(service = "cds"))
} else {
  login_check <- TRUE
}

test_that("set key", {
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)
  key <- system("echo $CDS", intern = TRUE)
  if(key != "" & key != "$CDS"){
    expect_message(wf_set_key(user = user,
               key = key,
               service = "cds"))
  }
  rm(key)
})

test_that("cds datasets returns data.frame or list", {
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)
  expect_true(inherits(wf_datasets(user = user,
                                   service = "cds",
                                   simplify = TRUE), "data.frame"))
  expect_true(inherits(wf_datasets(user = user,
                                   service = "cds",
                                   simplify = FALSE), "list"))
})

# Testing the cds request function
test_that("cds request", {
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)

  expect_message(wf_request(user = user,
                    request = cds_request,
                    transfer = TRUE))

  expect_error(wf_request(user = user,
                    request = "xyz",
                    transfer = TRUE))

  expect_error(wf_request(user = user,
                          transfer = TRUE))

  expect_true(inherits(wf_request(user = user,
              request = cds_request,
              transfer = FALSE), "wf_transfer"))
})


# # Expecting error if required arguments are not set:
 test_that("required arguments missing for cds_* functions", {
   skip_on_cran()
   skip_if(login_check)
   skip_if(server_check)

   # CDS dataset (requires at least 'user')
   expect_error(wf_dataset())
   expect_output(str(wf_datasets(user = user, service = "cds")))

   # CDS productinfo (requires at least 'user' and 'dataset')
   expect_error(wf_product_info())
   expect_error(wf_product_info(user = user,
                                service = "cds",
                                dataset = "foo"))

   # CDS productinfo: product name which is not available
   expect_output(str(wf_product_info(user = user,
                                     service = "cds",
                                     dataset = "satellite-methane")))

   # CDS tranfer (forwarded to wf_transfer, requires at least
   # 'user' and 'url)
   expect_error(wf_transfer())
   expect_error(wf_transfer(user = user,
                            service = "cds",
                            url = "http://google.com"))

   # CDS transfer with wrong type
   expect_error(wf_transfer(user = user,
                            url = "http://google.com",
                            service = "foo"))

   # check product listing
   expect_output(str(wf_product_info("reanalysis-era5-single-levels",
                                     service = "cds",
                                     user = NULL,
                                     simplify = FALSE)))

   expect_output(str(wf_product_info("reanalysis-era5-single-levels",
                                     service = "cds",
                                     user = NULL,
                                     simplify = FALSE)))
})

# check delete routine CDS (fails)
test_that("delete request", {
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)
   expect_warning(
     wf_delete(user = user,
               service = "cds",
               url = "50340909as"))
})

# CDS product info
test_that("check product info",{
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)
  expect_output(
    str(wf_product_info("reanalysis-era5-single-levels",
                        service = "cds",
                        user = NULL)))
})
