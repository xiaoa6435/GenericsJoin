#include <Rcpp.h>
#include <iostream>
using namespace std;
using namespace Rcpp;

inline bool gt(double x, double y){
  return (x > y);
}

inline bool ge(double x, double y){
  return (x >= y);
}

inline bool lt(double x, double y){
  return (x < y);
}

inline bool le(double x, double y){
  return (x <= y);
}

// [[Rcpp::export]]
List sort_merge_join(NumericVector r, NumericVector s, String cpr) {

  int nl = r.size();
  int nr = s.size();

  int i = 0;
  int j = 0;

  std::vector<int> x_loc;
  std::vector<int> y_loc;

  bool (*compare)(double, double) = (cpr == ">")?gt:((cpr == ">=")?ge:(cpr == "<")?lt:le);


  if (cpr == "==") {

    while (i < nl && j < nr) {
      if(r[i] == s[j]) {
        // if have tie, cross join it
        int j_rep = 0;
        do {
          j_rep = 0;
          do {
            x_loc.push_back(i + 1);
            y_loc.push_back(j + 1);
            j++;
            j_rep++;
          } while ((j < nr) && (s[j] == s[j - 1]));
          j = j - j_rep;
          i++;
        } while ((i < nl) && (r[i] == r[i - 1]));
      } else if(r[i] > s[j]) {
        j++;
      } else {
        i++;
      }
    }

  } else if (cpr == ">=" || cpr == ">") {
    while (i < nl) {
      if (compare(r[i], s[j])) {
        for (int k = 0; k <= j; k++){
          x_loc.push_back(i + 1);
          y_loc.push_back(k + 1);
        }
        while(j + 1 < nr && compare(r[i], s[j + 1])){
          j++;
          x_loc.push_back(i + 1);
          y_loc.push_back(j + 1);
        }
      }
      i++;
    }

  } else {
    while (j < nr) {
      if (compare(r[i], s[j])) {
        for (int k = 0; k <= i; k++){
          x_loc.push_back(k + 1);
          y_loc.push_back(j + 1);
        }
        while(i + 1 < nr && compare(r[i + 1], s[j])){
          i++;
          x_loc.push_back(i + 1);
          y_loc.push_back(j + 1);
        }
      }
      j++;
    }
  }

  return List::create(
    _["x_loc"] = x_loc,
    _["y_loc"] = y_loc
  );
}

/*** R
sort_merge_join(c(1, 1, 2, 2, 4), c(1, 2, 2, 2, 4), cpr = '==')


  sort_merge_join(c(1, 1, 2, 2, 4), c(1, 2, 2, 2, 4), cpr = '==')


  sort_merge_join(c(1, 1, 2, 4), c(1, 2, 2, 4), cpr = '<=')
  sort_merge_join(c(1, 1, 2, 4), c(1, 2, 2, 4), cpr = '<')
  sort_merge_join(c(1, 1, 2, 4), c(1, 2, 2, 4), cpr = '>')
  sort_merge_join(c(1, 1, 2, 4), c(1, 2, 2, 4), cpr = '>=')

# r <- c(1, 2, 2, 3, 4, 5)
# s <- c(1, 1, 3, 3, 4, 4)
# sort_merge_join(c(1, 2, 2, 3, 4, 5), c(1, 1, 3, 3, 4, 4), cpr = '==')

# sort_merge_join(r = c(2, 5, 6, 7, 9, 11), s = c(3, 4, 7, 7, 10, 10), cpr = '==')
# sort_merge_join(r = c(2, 5, 6, 7, 9, 11), s = c(3, 4, 7, 7, 10, 10), cpr = '>')
# sort_merge_join(r = c(2, 5, 6, 7, 9, 11), s = c(3, 4, 7, 7, 10, 10), cpr = '>=')
# x > y
# list(
#   x_loc = c(2, 3, 4, 5, 6),
#   y_loc = c(2, 2, 4, 4, 6)
# )
# do.call(tibble, res) %>%
#   dplyr::group_by(x_loc) %>%
#   dplyr::summarise(max(y_loc))
#
# r <- c(2, 5, 6, 7, 9, 11)
# s <- c(3, 4, 7, 7, 10, 10)
# rowid_to_column(tibble(x = r)) %>%
#   dplyr::inner_join(rowid_to_column(tibble(y = s)), by = character()) %>%
#   dplyr::filter(x > y) %>%
#   group_by(rowid.x) %>%
#   summarise(max(rowid.y))
#
# x >= y
# cut point
# list(
#   x_loc = c(2, 3, 4, 5, 6),
#   y_loc = c(2, 2, 2, 4, 6)
# )
#
# do.call(tibble, res) %>%
#   dplyr::group_by(x_loc) %>%
#   dplyr::summarise(max(y_loc))
  */


/*** R
sort_merge_join(c(1, 1, 2, 2, 4), c(1, 2, 2, 2, 4), cpr = '==')
sort_merge_join(c(1, 1, 2, 2, 4), c(1, 2, 2, 2, 4), cpr = '==')
sort_merge_join(c(1, 1, 2, 4), c(1, 2, 2, 4), cpr = '<=')
sort_merge_join(c(1, 1, 2, 4), c(1, 2, 2, 4), cpr = '<')
sort_merge_join(c(1, 1, 2, 4), c(1, 2, 2, 4), cpr = '>')
sort_merge_join(c(1, 1, 2, 4), c(1, 2, 2, 4), cpr = '>=')
*/
