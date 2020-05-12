join_by <- function(...){
  enquos(...)
}

standardise_join_by_expr <- function(quo, x, y){

  expr <- get_expr(quo)
  x_names <- dplyr::tbl_vars(x)
  y_names <- dplyr::tbl_vars(y)
  x_names_with_suffix <- paste0(x_names, '.x')
  y_names_with_suffix <- paste0(y_names, '.y')

  cpr <- rlang::call_name(expr)
  all_keys <- all.vars(expr)
  lhs_keys <- all.vars(expr[[2]])
  rhs_keys <- all.vars(expr[[3]])

  ## a simple unequal expr like this: t1.x + 3 < t2.y, all sym in lhs all from tibble x and
  ## compare operation not '!=' (bacause sort merge join can't handle != join)
  ##
  ## a complex on expression can be anything f(x, y) return bool value, for example
  ## (lng.x - lng.y) ^ 2 + (lat.x - lat.y) ^ 2 < 5, x y ara join table, this situation
  ## must be a nest loop join

  is_simple_expr <- (
    is.null(names(expr)) &&
      cpr %in% c('>', '>=', '==', '<', '<=') &&
      all(lhs_keys %in% c(x_names, x_names_with_suffix)) &&
      all(rhs_keys %in% c(y_names, x_names_with_suffix))
  )

  lhs <- expr[[2]]
  rhs <- expr[[3]]
  if (is_simple_expr) {
    need_rename <- all_keys[all_keys %in% c(x_names_with_suffix, y_names_with_suffix)]
    for (i in seq_along(need_rename)) {
      raw <- need_rename[i]
      new <- substring(raw, 1, nchar(raw) - 2)
      expr <- expr_substitute(expr, as.symbol(raw), as.symbol(new))
    }
  } else {
    need_rename <- all_keys[all_keys %in% c(x_names, y_names)]
    for (i in seq_along(need_rename)) {
      raw <- need_rename[i]
      new <- ifelse(raw %in% x_names, paste0(raw, '.x'), paste0(raw, '.y'))
      expr <- expr_substitute(expr, as.symbol(raw), as.symbol(new))
    }
  }

  lhs <- expr[[2]]
  rhs <- expr[[3]]
  if (is_simple_expr) {
    x_key <- rlang::eval_tidy(lhs, x)
    y_key <- rlang::eval_tidy(rhs, y)
    on <- cpr
  } else {
    all_keys <- all.vars(expr)
    x_key <- x[x_names_with_suffix %in% all_keys]
    y_key <- y[y_names_with_suffix %in% all_keys]
    on <- expr
  }

  by <- list(x_key = x_key, y_key = y_key, on = on)
  if (is_simple_expr && cpr == '==') {
    class(by) <- 'hash_join'
  } else if (is_simple_expr) {
    class(by) <- 'sort_merge_join'
  } else if (!is_simple_expr) {
    class(by) <- 'nest_loop_join'
  }

  by

}

# standardise_join_by_expr <- function(x_names, y_names, e){
#   # browser()
#   x_names_s <- paste0(x_names, '.x')
#   y_names_s <-  paste0(y_names, '.y')
#
#   cpr <- node_car(e)
#   lhs <- node_cadr(e)
#   rhs <- node_cddr(e)
#
#   x <- find_para_name(lhs)
#   y <- find_para_name(rhs)
#
#   ## a simplce unequal expr like this: t1.x + 3 < t2.y, all sym in lhs all from tibble x and
#   ## compare operation not '!=' (bacause sort merge join can't handle != join)
#   ##
#   ## a complex on expression can be anything f(x, y) return bool value, for example
#   ## (lng.x - lng.y) ^ 2 + (lat.x - lat.y) ^ 2 < 5, x y ara join table, this situation
#   ## must be a nest loop join
#   ##
#   is_simple_expr <- (
#     as_string(cpr) %in% c('>', '>=', '==', '<', '<=') &&
#     all(x %in% c(x_names, x_names_s)) &&
#     all(y %in% c(y_names, y_names_s))
#   )
#
#   paras <- find_para_name(e)
#   if (is_simple_expr) {
#     need_rename <- paras[c(x, y) %in% c(x_names_s, y_names_s)]
#     for (i in seq_along(need_rename)) {
#       raw <- need_rename[i]
#       new <- substring(raw, 1, nchar(raw) - 2)
#       e <- expr_substitute(e, as.symbol(raw), as.symbol(new))
#     }
#   } else {
#     need_rename <- paras[c(x, y) %in% c(x_names, y_names)]
#     for (i in seq_along(need_rename)) {
#       raw <- need_rename[i]
#       new <- ifelse(raw %in% x_names, paste0(raw, '.x'), paste0(raw, '.y'))
#       e <- expr_substitute(e, as.symbol(raw), as.symbol(new))
#     }
#   }
#
#   if (is_simple_expr) {
#     x <- node_cadr(e)
#     y <- node_cddr(e)[[1]]
#     # x <- find_para_name(lhs)
#     # y <- find_para_name(rhs)
#   } else {
#     keys <- find_para_name(e)
#     x <- intersect(keys, x_names_s)
#     y <- intersect(keys, y_names_s)
#     x <- syms(substring(x, 1, nchar(x) - 2))
#     y <- syms(substring(y, 1, nchar(y) - 2))
#
#   }
#
#   by <- list(x = x, y = y, on = e)
#
#   if (is_simple_expr && cpr == '==') {
#     class(by) <- 'equ'
#   } else if (is_simple_expr) {
#     class(by) <- 'inequ'
#   } else if (!is_simple_expr) {
#     class(by) <- 'inequ_mix'
#   }
#   by
# }

# find_para_name <- function(x) {
#   res <- if (is.call(x) || is_pairlist(x)) {
#     purrr::flatten_chr(purrr::map(x[2:length(x)], find_para_name))
#   } else if (is.symbol(x)) {
#     as_string(x)
#   } else {
#     NULL
#   }
#   unique(res)
# }
