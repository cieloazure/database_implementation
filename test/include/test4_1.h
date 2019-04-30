#ifndef TEST_H
#define TEST_H
extern "C" {
int yyparse(void);  // defined in y.tab.c
struct YY_BUFFER_STATE *yy_scan_string(const char *);
}
#endif