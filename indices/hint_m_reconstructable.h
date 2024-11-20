// Reconstructable HINT index supporting tombstones and rebuilding.
// It extends HINT_M_Dynamic to facilate fossil indexing,

#ifndef HINT_M_RECONSTRUCTABLE_H
#define HINT_M_RECONSTRUCTABLE_H

#include "hint_m.h"


class HINT_Reconstructable : public HINT_M_Dynamic {
public:
    // Constructor
    HINT_Reconstructable(Timestamp leafPartitionExtent);

    // Rebuild the index to remove tombstoned records
    void rebuild();

    // Check if a record is tombstoned
    bool isTombstoned(const Record& r) const;

    void clear();
};

#endif // HINT_M_RECONSTRUCTABLE_H
