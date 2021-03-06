// Generated by using Rcpp::compileAttributes() -> do not edit by hand
// Generator token: 10BE3573-1514-4C36-9D1C-5A225CD40393

#include <Rcpp.h>

using namespace Rcpp;

// sort_merge_join
List sort_merge_join(NumericVector r, NumericVector s, String cpr);
RcppExport SEXP _GenericsJoin_sort_merge_join(SEXP rSEXP, SEXP sSEXP, SEXP cprSEXP) {
BEGIN_RCPP
    Rcpp::RObject rcpp_result_gen;
    Rcpp::RNGScope rcpp_rngScope_gen;
    Rcpp::traits::input_parameter< NumericVector >::type r(rSEXP);
    Rcpp::traits::input_parameter< NumericVector >::type s(sSEXP);
    Rcpp::traits::input_parameter< String >::type cpr(cprSEXP);
    rcpp_result_gen = Rcpp::wrap(sort_merge_join(r, s, cpr));
    return rcpp_result_gen;
END_RCPP
}

static const R_CallMethodDef CallEntries[] = {
    {"_GenericsJoin_sort_merge_join", (DL_FUNC) &_GenericsJoin_sort_merge_join, 3},
    {NULL, NULL, 0}
};

RcppExport void R_init_GenericsJoin(DllInfo *dll) {
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
}
