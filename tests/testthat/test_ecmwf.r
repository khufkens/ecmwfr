# set options
opts <- options(keyring_warn_for_env_fallback = FALSE)
on.exit(options(opts), add = TRUE)
user <- "khrdev@outlook.com"
user <- "eliocampitelli@gmail.com"
# format request (see below)
my_request <- list(stream = "oper",
                   levtype = "sfc",
                   param = "165.128",
                   dataset = "interim",
                   step = "0",
                   grid = "0.75/0.75",
                   time = "00",
                   date = "2014-07-01",
                   type = "an",
                   class = "ei",
                   area = "51/0/50/1",
                   format = "netcdf",
                   target = "tmp.nc")

# is the server reachable
server_check <- !ecmwf_running(wf_server(service = "webapi"))

# if server is up, create login
if(!server_check){
  key <- system("echo $KEY", intern = TRUE)
  if(key != "" & key != "$KEY"){
    wf_set_key(user = user,
               key = system("echo $KEY", intern = TRUE),
               service = "webapi")
  }
  rm(key)

  login_check <- try(wf_get_key(user = user), silent = TRUE)
  login_check <- inherits(login_check, "try-error")
} else {
  login_check <- TRUE
}

test_that("set, get secret key",{
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)

  # check retrieval
  expect_output(str(wf_get_key(user = user)))

  # failed set keys commands
  expect_error(wf_set_key(key = "XXXX",
                          service = "webapi"))
  expect_error(wf_set_key(user = user,
                          service = "webapi"))
  expect_error(wf_set_key(user = user))
})

test_that("test dataset function", {
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)
  expect_output(str(wf_datasets(user = user)))
})

test_that("test dataset function - no login", {
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)
  expect_error(wf_datasets())
})

test_that("list datasets webapi",{
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)
  expect_output(str(wf_datasets(user = user,
                                service = "webapi")))
})

test_that("test services function", {
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)
  expect_output(str(wf_services(user = user)))
})

test_that("test services function - no login", {
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)
  expect_error(wf_services())
})

test_that("test user info function", {
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)
  expect_output(str(wf_user_info(user = user)))
})

test_that("test user info function - no login", {
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)
  expect_error(wf_user_info())
})

test_that("test request (transfer) function", {
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)
  expect_message(wf_request(
    user = user,
    transfer = TRUE,
    request = my_request,
    time_out = 60))
})

test_that("test request (transfer) function", {
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)

  # create new output dir in tempdir()
  path <- file.path(tempdir(),"/test/")
  dir.create(path = path)

  expect_message(wf_request(
    user = user,
    transfer = TRUE,
    request = my_request,
    path = path))
})

test_that("test request (transfer) function - time out", {
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)
  expect_output(str(wf_request(
    user = user,
    transfer = TRUE,
    request = my_request,
    time_out = 1)))
})

test_that("test request (transfer) function - no transfer", {
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)
  ct <- wf_request(
    user = user,
    transfer = FALSE,
    request = my_request)

  ct2 <- wf_request(
    user = user,
    transfer = FALSE,
    request = my_request)


  expect_output(str(ct))
  expect_message(wf_delete(user = user,
                           url = ct$href))
  expect_silent(wf_delete(user = user,
                          url = ct2$href,
                          verbose = FALSE))
})

test_that("test request (transfer) function - no email", {
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)
  expect_error(wf_request())
})

test_that("test transfer function - no login", {
  skip_if(login_check)
  skip_if(server_check)
  expect_error(wf_transfer())
})

test_that("list datasets webapi",{
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)
  expect_output(str(wf_datasets(user = user,
                                service = "webapi")))
  expect_output(str(wf_datasets(user = user,
                                service = "webapi",
                                simplify = FALSE)))
})

test_that("test request (transfer) function", {
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)
  expect_message(
    wf_request(
      user = user,
      transfer = TRUE,
      request = my_request,
      time_out = 180)
  )
})

# webapi product info
test_that("check product info",{
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)
  expect_output(str(
    wf_product_info(dataset = "interim",
                    user = user,
                    service = "webapi",
                    simplify = FALSE)))
})

test_that("test delete function - no login", {
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)
  expect_error(wf_delete())
})

test_that("test request (transfer) function - larger download", {
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)

  # large request
  large_request <- list(stream = "oper",
                        levtype = "sfc",
                        param = "167.128",
                        dataset = "interim",
                        step = "0",
                        grid = "0.75/0.75",
                        time = "00",
                        date = "2014-07-01",
                        type = "an",
                        class = "ei",
                        area = "50/10/55/15",
                        format = "netcdf",
                        target = "tmp.nc")

  expect_message(wf_request(
    user = user,
    transfer = TRUE,
    request = large_request,
    time_out = 300))
})

test_that("check request - no dataset field", {
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)

  my_request <- list(stream = "oper",
                     levtype = "sfc",
                     param = "167.128",
                     step = "0",
                     grid = "0.75/0.75",
                     time = "00",
                     date = "2014-07-01",
                     type = "an",
                     class = "ei",
                     area = "51/0/50/1",
                     format = "netcdf")
  expect_error(
    wf_check_request(
      user = user,
      request = my_request)
  )
})

test_that("check request - bad request type", {
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)

  my_request <- "xyz"
  expect_error(
    wf_check_request(
      user = user,
      request = my_request)
  )
})

test_that("check mars request - no target", {
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)

  my_request <- list(stream = "oper",
                     levtype = "sfc",
                     param = "167.128",
                     dataset = "mars",
                     step = "0",
                     grid = "0.75/0.75",
                     time = "00",
                     date = "2014-07-01",
                     type = "an",
                     class = "ei",
                     area = "50/10/61/21",
                     format = "netcdf")
  expect_error(
    wf_check_request(
      user = user,
      request = my_request)
  )
})

test_that("check request - no netcdf grid specified", {
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)

  my_request <- list(stream = "oper",
                     levtype = "sfc",
                     param = "167.128",
                     dataset = "mars",
                     step = "0",
                     time = "00",
                     date = "2014-07-01",
                     type = "an",
                     class = "ei",
                     area = "50/10/55/15",
                     format = "netcdf")
  expect_error(
    wf_check_request(
      user = user,
      request = my_request)
  )
})

test_that("check request - bad credentials", {
  skip_on_cran()
  skip_if(login_check)
  skip_if(server_check)

  my_request <- list(stream = "oper",
                     levtype = "sfc",
                     param = "167.128",
                     dataset = "interim",
                     step = "0",
                     grid = "0.75/0.75",
                     time = "00",
                     date = "2014-07-01",
                     type = "an",
                     class = "ei",
                     area = "50/10/61/21",
                     format = "netcdf",
                     target = "tmp.nc")
  expect_error(
    wf_check_request(
      user = "zzz@zzz.zzz",
      request = my_request)
  )
})

test_that("job_name has to be valid", {
  my_request <- list(stream = "oper",
                     levtype = "sfc",
                     param = "167.128",
                     dataset = "interim",
                     step = "0",
                     grid = "0.75/0.75",
                     time = "00",
                     date = "2014-07-01",
                     type = "an",
                     class = "ei",
                     area = "50/10/61/21",
                     format = "netcdf",
                     target = "tmp.nc")

  expect_error(
    wf_request(my_request,
               user = user,
               job_name = "1"),
    "job_name '1' is not a syntactically valid variable name.")
})
