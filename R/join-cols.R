join_cols <- function(x_names, y_names, by = NULL, suffix = c(".x", ".y"), keep = FALSE) {

  check_duplicate_vars(x_names, "x")
  check_duplicate_vars(y_names, "y")

  suffix <- standardise_join_suffix(suffix)

  if (class(by) == 'quosure') {
    x_by <- NULL
    y_by <- NULL
  } else {
    x_by <- set_names(match(by$x, x_names), by$x)
    y_by <- set_names(match(by$y, y_names), by$x)
  }

  x_loc <- seq_along(x_names)
  names(x_loc) <- x_names

  if (!keep) {
    # in x_out, key variables need to keep the same name, and aux
    # variables need suffixes for duplicates that appear in y_out
    y_aux <- setdiff(y_names, c(by$x, if (!keep) by$y))
    x_is_aux <- !x_names %in% by$x
    names(x_loc)[x_is_aux] <- add_suffixes(x_names[x_is_aux], c(by$x, y_aux), suffix$x)
  } else {
    # in x_out, key variables and aux variables need suffixes
    # for duplicates that appear in y_out
    names(x_loc) <- add_suffixes(x_names, y_names, suffix$x)
  }

  y_loc <- seq_along(y_names)
  names(y_loc) <- add_suffixes(y_names, x_names, suffix$y)
  if (!keep) {
    y_loc <- y_loc[!y_names %in% by$y]
  }

  # key = named location to use for matching
  # out = named locations to use in output
  list(
    x = list(key = x_by, out = x_loc),
    y = list(key = y_by, out = y_loc)
  )
}

check_join_vars <- function(vars, names) {
  if (!is.character(vars)) {
    abort("join columns must be character vectors")
  }

  na <- is.na(vars)
  if (any(na)) {
    abort(glue_c(
      "Join columns must be not NA",
      x = "Problem at position {err_vars(na)}"
    ))
  }

  dup <- duplicated(vars)
  if (any(dup)) {
    abort(glue_c(
      "Join columns must be unique",
      x = "Problem at position {err_vars(dup)}"
    ))
  }

  missing <- setdiff(vars, names)
  if (length(missing) > 0) {
    abort(glue_c(
      "Join columns must be present in data",
      x = "Problem with {err_vars(missing)}"
    ))
  }
}

check_duplicate_vars <- function(vars, input) {
  dup <- duplicated(vars)
  if (any(dup)) {
    abort(glue_c(
      "Input columns in `{input}` must be unique",
      x = "Problem with {err_vars(vars[dup])}"
    ))
  }
}

standardise_join_suffix <- function(x) {
  if (!is.character(x) || length(x) != 2) {
    abort(glue_c(
      "`suffix` must be a character vector of length 2",
      i = "suffix is {friendly_type_of(x)} of length {length(x)}"
    ))
  }

  if (any(is.na(x))) {
    bad_args("suffix", "can't be NA")
  }

  list(x = x[[1]], y = x[[2]])
}

add_suffixes <- function(x, y, suffix) {
  if (identical(suffix, "")) {
    return(x)
  }

  out <- rep_along(x, na_chr)
  for (i in seq_along(x)) {
    nm <- x[[i]]
    while (nm %in% y || nm %in% out[seq_len(i - 1)]) {
      nm <- paste0(nm, suffix)
    }

    out[[i]] <- nm
  }
  out
}
