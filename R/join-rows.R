join_rows <- function(join_keys, na_equal) UseMethod("join_rows", join_keys)

join_rows.sort_merge_join <- function(join_keys, na_equal = TRUE) {
  r <- join_keys$x_key
  s <- join_keys$y_key
  on <- join_keys$on

  x_order <- order(r)
  y_order <- order(s)

  res <- sort_merge_join(r[x_order], s[y_order], on)
  x_loc <- seq_len(vec_size(r))[x_order][res$x_loc]
  y_loc <- seq_len(vec_size(s))[y_order][res$y_loc]

  list(x = x_loc, y = y_loc)
}

join_rows.nest_loop_join <- function(join_keys, na_equal = TRUE){
  # browser()
  x_key <- join_keys$x_key
  y_key <- join_keys$y_key
  x_key <- rlang::set_names(x_key, nm = paste0(names(x_key), '.x'))
  y_key <- rlang::set_names(y_key, nm = paste0(names(y_key), '.y'))

  on <- join_keys$on

  x_split <- set_names(vec_group_loc(x_key), c('x', 'loc.x'))
  y_split <- set_names(vec_group_loc(y_key), c('y', 'loc.y'))

  nkey_x <- vec_size(x_split)
  nkey_y <- vec_size(y_split)

  x_key_id <- seq_len(nkey_x)
  y_key_id <- seq_len(nkey_y)

  ## cross x_key * x_key then filter on condition
  x_key_loc <- rep(x_key_id, times = nkey_y)
  y_key_loc <- rep(y_key_id, each  = nkey_x)
  out <- vec_cbind(vec_slice(x_split, x_key_loc), vec_slice(y_split, y_key_loc))
  out <- out %>% tidyr::unpack(x) %>% tidyr::unpack(y)
  out <- dplyr::filter(out, !!!on)

  ## unchop loc.x and loc.y
  out <- out %>% tidyr::unchop(loc.x) %>% tidyr::unchop(loc.y)

  list(x = out$loc.x, y = out$loc.y)
}

join_rows.hash_join <- function(join_keys, na_equal = TRUE) {

  x_key <- join_keys$x_key
  y_key <- join_keys$y_key

  # Find matching rows in y for each row in x
  y_split <- vec_group_loc(y_key)
  tryCatch(
    matches <- vec_match(x_key, y_split$key, na_equal = na_equal),
    vctrs_error_incompatible_type = function(cnd) {
      rx <- "^[^$]+[$]"
      x_name <- sub(rx, "", cnd$x_arg)
      y_name <- sub(rx, "", cnd$y_arg)

      abort(c(
        glue("Can't join on `x${x_name}` x `y${y_name}` because of incompatible types. "),
        i = glue("`x${x_name}` is of type <{x_type}>>.", x_type = vec_ptype_full(cnd$x)),
        i = glue("`y${y_name}` is of type <{y_type}>>.", y_type = vec_ptype_full(cnd$y))
      ))
    }
  )

  y_loc <- y_split$loc[matches]
  x_loc <- seq_len(vec_size(x_key))

  # flatten index list
  x_loc <- rep(x_loc, lengths(y_loc))
  y_loc <- index_flatten(y_loc)
  y_loc <- if (rlang::is_null(y_loc)) integer() else y_loc

  list(x = x_loc, y = y_loc)
}

# TODO: Replace with `vec_unchop(x, ptype = integer())`
# once performance of `vec_c()` matches `unlist()`. See #4964.
index_flatten <- function(x) {
  unlist(x, recursive = FALSE, use.names = FALSE)
}

# join_rows.sort_merge_join <- function(join_keys, na_equal = TRUE) {
#
#   # browser()
#   x_key <- join_keys$x_key
#   y_key <- join_keys$y_key
#   on <- join_keys$on
#
#   x_raw_rowid <- seq_len(vec_size(x_key))
#   y_row_rowid <- seq_len(vec_size(y_key))
#   nkey <- ncol(x_key)
#   out <- list()
#   for (i in seq(nkey)) {
#
#     r <- x_key[, i, drop = TRUE]
#     s <- y_key[, i, drop = TRUE]
#
#     x_order <- order(r)
#     y_order <- order(s)
#     # res <- sort_merge_join(1:3, 1:3, '<=')
#     res <- sort_merge_join(r[x_order], s[y_order], on[i])
#     x_loc <- x_raw_rowid[x_order][res$x_loc]
#     y_loc <- y_row_rowid[y_order][res$y_loc]
#     out[[i]] <- tibble(x_loc = x_loc, y_loc = y_loc)
#   }
#   out <- reduce(out, dplyr::intersect)
#
#   list(x = out$x_loc, y = out$y_loc)
# }
