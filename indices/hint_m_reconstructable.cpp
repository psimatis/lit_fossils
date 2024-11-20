#include "hint_m_reconstructable.h"

// Constructor
HINT_Reconstructable::HINT_Reconstructable(Timestamp leafPartitionExtent)
    : HINT_M_Dynamic(leafPartitionExtent) {}

// Check if a record is tombstoned
bool HINT_Reconstructable::isTombstoned(const Record& r) const {
    // Example implementation: Adjust based on your tombstone mechanism
    return r.id == -1; // Assuming an ID of -1 marks a record as tombstoned.
}

void HINT_Reconstructable::clear() {
    // Reset vectors
    pOrgsInIds.clear();
    pOrgsAftIds.clear();
    pRepsInIds.clear();
    pRepsAftIds.clear();

    pOrgsInTimestamps.clear();
    pOrgsAftTimestamps.clear();
    pRepsInTimestamps.clear();
    pRepsAftTimestamps.clear();

    // Reset size variables
    pOrgsIn_sizes = nullptr;
    pOrgsAft_sizes = nullptr;
    pRepsIn_sizes = nullptr;
    pRepsAft_sizes = nullptr;

    // Reset global timestamps
    gstart = 0;
    gend = leafPartitionExtent;
}


// Rebuild the index
void HINT_Reconstructable::rebuild() {
    // Temporary container to collect all valid records
    Relation validRecords;

    // Collect valid records from pOrgsInIds
    for (size_t level = 0; level < this->height; ++level) {
        for (size_t partition = 0; partition < this->pOrgsInIds[level].size(); ++partition) {
            for (size_t i = 0; i < this->pOrgsInIds[level][partition].size(); ++i) {
                RelationId id;
                id.push_back(this->pOrgsInIds[level][partition][i]);
                const auto& timestamp = this->pOrgsInTimestamps[level][partition][i];
                Record r = {id[0], timestamp.first, timestamp.second};

                if (!isTombstoned(r)) {
                    validRecords.push_back(r);
                }
            }
        }
    }

    // Repeat for pOrgsAft, pRepsIn, pRepsAft
    for (size_t level = 0; level < this->height; ++level) {
        for (size_t partition = 0; partition < this->pOrgsAftIds[level].size(); ++partition) {
            for (size_t i = 0; i < this->pOrgsAftIds[level][partition].size(); ++i) {
                RelationId id;
                id.push_back(this->pOrgsAftIds[level][partition][i]);
                const auto& timestamp = this->pOrgsAftTimestamps[level][partition][i];
                Record r = {id[0], timestamp.first, timestamp.second};

                if (!isTombstoned(r)) {
                    validRecords.push_back(r);
                }
            }
        }

        for (size_t partition = 0; partition < this->pRepsInIds[level].size(); ++partition) {
            for (size_t i = 0; i < this->pRepsInIds[level][partition].size(); ++i) {
                RelationId id;
                id.push_back(this->pRepsInIds[level][partition][i]);
                const auto& timestamp = this->pRepsInTimestamps[level][partition][i];
                Record r = {id[0], timestamp.first, timestamp.second};

                if (!isTombstoned(r)) {
                    validRecords.push_back(r);
                }
            }
        }

        for (size_t partition = 0; partition < this->pRepsAftIds[level].size(); ++partition) {
            for (size_t i = 0; i < this->pRepsAftIds[level][partition].size(); ++i) {
                RelationId id;
                id.push_back(this->pRepsAftIds[level][partition][i]);
                const auto& timestamp = this->pRepsAftTimestamps[level][partition][i];
                Record r = {id[0], timestamp.first, timestamp.second};

                if (!isTombstoned(r)) {
                    validRecords.push_back(r);
                }
            }
        }
    }

    // Create a new index
    HINT_Reconstructable newIndex(this->leafPartitionExtent);

    // Reinsert all valid records into the new index
    for (const auto& record : validRecords) {
        newIndex.insert(record);
    }

    // Replace the current index with the new index
    *this = std::move(newIndex);
}
