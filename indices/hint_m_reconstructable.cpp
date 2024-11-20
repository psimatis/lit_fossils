#include "hint_m_reconstructable.h"

using namespace std;

HINT_Reconstructable::HINT_Reconstructable(Timestamp leafPartitionExtent)
    : HINT_M_Dynamic(leafPartitionExtent) {}

bool HINT_Reconstructable::isFossil(const Record& r, Timestamp Tf) {
    return r.end <= Tf; // Mark as tombstoned if the record's end timestamp is less than or equal to Tf
}

// Reconstructs the HINT without the tombstones intervals.
// The tombstoned records are returned so the fossil index can insert them.
Relation HINT_Reconstructable::rebuild(Timestamp Tf) {
    Relation validRecords;
    Relation fossilRecords;

    // Track processed intervals to prevent duplicates
    unordered_set<int> processedIds;

    // Iterate through all partitions and separate intervals
    for (size_t level = 0; level < this->height; ++level) {
        for (size_t partition = 0; partition < this->pOrgsInIds[level].size(); ++partition) {
            for (size_t i = 0; i < this->pOrgsInIds[level][partition].size(); ++i) {
                RelationId id;
                id.push_back(this->pOrgsInIds[level][partition][i]);
                const auto& timestamp = this->pOrgsInTimestamps[level][partition][i];
                Record r = {id[0], timestamp.first, timestamp.second};

                if (processedIds.count(r.id)) continue;
                processedIds.insert(r.id);

                if (isFossil(r, Tf)) fossilRecords.push_back(r);
                else validRecords.push_back(r);
            }
        }

        // Repeat for pOrgsAft, pRepsIn, pRepsAft
        for (size_t partition = 0; partition < this->pOrgsAftIds[level].size(); ++partition) {
            for (size_t i = 0; i < this->pOrgsAftIds[level][partition].size(); ++i) {
                RelationId id;
                id.push_back(this->pOrgsAftIds[level][partition][i]);
                const auto& timestamp = this->pOrgsAftTimestamps[level][partition][i];
                Record r = {id[0], timestamp.first, timestamp.second};

                if (processedIds.count(r.id)) continue;
                processedIds.insert(r.id);

                if (isFossil(r, Tf)) fossilRecords.push_back(r);
                else validRecords.push_back(r);
            }
        }

        for (size_t partition = 0; partition < this->pRepsInIds[level].size(); ++partition) {
            for (size_t i = 0; i < this->pRepsInIds[level][partition].size(); ++i) {
                RelationId id;
                id.push_back(this->pRepsInIds[level][partition][i]);
                const auto& timestamp = this->pRepsInTimestamps[level][partition][i];
                Record r = {id[0], timestamp.first, timestamp.second};

                if (processedIds.count(r.id)) continue;
                processedIds.insert(r.id);

                if (isFossil(r, Tf)) fossilRecords.push_back(r);
                else validRecords.push_back(r);
            }
        }

        for (size_t partition = 0; partition < this->pRepsAftIds[level].size(); ++partition) {
            for (size_t i = 0; i < this->pRepsAftIds[level][partition].size(); ++i) {
                RelationId id;
                id.push_back(this->pRepsAftIds[level][partition][i]);
                const auto& timestamp = this->pRepsAftTimestamps[level][partition][i];
                Record r = {id[0], timestamp.first, timestamp.second};

                if (processedIds.count(r.id)) continue;
                processedIds.insert(r.id);

                if (isFossil(r, Tf)) fossilRecords.push_back(r);
                else validRecords.push_back(r);
            }
        }
    }

    // Create a new HINT index with remaining valid records
    if (fossilRecords.size() > 0) {
        HINT_Reconstructable newIndex(this->leafPartitionExtent);
        for (const auto& record : validRecords) newIndex.insert(record);
        *this = move(newIndex);
    }
    return fossilRecords;
}

size_t HINT_Reconstructable::getMemoryUsage() const {

    size_t totalSize = 0;

    // Memory for pOrgsInIds and related timestamps
    for (const auto& level : pOrgsInIds) {
        for (const auto& partition : level) {
            totalSize += partition.size() * sizeof(RecordId); // Memory for RecordIds
        }
    }
    for (const auto& level : pOrgsInTimestamps) {
        for (const auto& partition : level) {
            totalSize += partition.size() * sizeof(pair<Timestamp, Timestamp>); // Memory for timestamps
        }
    }

    // Repeat for pOrgsAft, pRepsIn, pRepsAft
    for (const auto& level : pOrgsAftIds) {
        for (const auto& partition : level) {
            totalSize += partition.size() * sizeof(RecordId);
        }
    }
    for (const auto& level : pOrgsAftTimestamps) {
        for (const auto& partition : level) {
            totalSize += partition.size() * sizeof(pair<Timestamp, Timestamp>);
        }
    }
    for (const auto& level : pRepsInIds) {
        for (const auto& partition : level) {
            totalSize += partition.size() * sizeof(RecordId);
        }
    }
    for (const auto& level : pRepsInTimestamps) {
        for (const auto& partition : level) {
            totalSize += partition.size() * sizeof(pair<Timestamp, Timestamp>);
        }
    }
    for (const auto& level : pRepsAftIds) {
        for (const auto& partition : level) {
            totalSize += partition.size() * sizeof(RecordId);
        }
    }
    for (const auto& level : pRepsAftTimestamps) {
        for (const auto& partition : level) {
            totalSize += partition.size() * sizeof(pair<Timestamp, Timestamp>);
        }
    }

    return totalSize;
}

