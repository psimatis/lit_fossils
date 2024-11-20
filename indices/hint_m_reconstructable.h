// Reconstructable HINT supporting rebuilding.
// It extends HINT_M_Dynamic to facilate fossil indexing,

#ifndef HINT_M_RECONSTRUCTABLE_H
#define HINT_M_RECONSTRUCTABLE_H

#include "hint_m.h"


class HINT_Reconstructable : public HINT_M_Dynamic {
public:
    HINT_Reconstructable(Timestamp leafPartitionExtent);
    void rebuild(); // Rebuild index without tombstones
    bool isTombstoned(const Record& r) const;
};

#endif // HINT_M_RECONSTRUCTABLE_H
