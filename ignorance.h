#ifndef IGNORANCE_H
#define IGNORANCE_H

extern bool aqo_log_ignorance;

extern void set_ignorance(bool newval, void *extra);
extern bool create_ignorance_table(bool fail_ok);
extern void update_ignorance(uint64 qhash, uint64 fhash, int fss_hash, Plan *plan);//?

#endif /* IGNORANCE_H */
