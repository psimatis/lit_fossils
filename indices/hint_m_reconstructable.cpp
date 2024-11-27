#include "hint_m_reconstructable.h"

using namespace std;

HINT_Reconstructable::HINT_Reconstructable(Timestamp leafPartitionExtent)
    : HINT_M_Dynamic(leafPartitionExtent) {}

bool isFossil(const Record& r, Timestamp Tf) {
    return r.end <= Tf; // Mark as tombstoned if the record's end timestamp is less than or equal to Tf
}

// Reconstructs the HINT without the fossils.
void processPartition(vector<int>& ids, vector<pair<Timestamp, Timestamp>>& timestamps, Timestamp Tf, unordered_set<int>& processed, Relation& valid, Relation& fossils) {
    for (size_t i = 0; i < ids.size(); ++i) {
        if (processed.count(ids[i])) continue; // Skip processed IDs
        processed.insert(ids[i]);

        const auto& timestamp = timestamps[i];
        Record r = {ids[i], timestamp.first, timestamp.second};

        if (isFossil(r, Tf)) fossils.push_back(r);
        else valid.push_back(r);
    }
}

Relation HINT_Reconstructable::rebuild(Timestamp Tf) {
    Relation valid, fossils;
    unordered_set<int> processedIds; // Prevent duplicates

    // Iterate through all partitions and separate intervals
    for (size_t level = 0; level < this->height; ++level) {
        for (size_t partition = 0; partition < this->pOrgsInIds[level].size(); ++partition) 
            processPartition(this->pOrgsInIds[level][partition], this->pOrgsInTimestamps[level][partition], Tf, processedIds, valid, fossils);
        for (size_t partition = 0; partition < this->pOrgsAftIds[level].size(); ++partition) 
            processPartition(this->pOrgsAftIds[level][partition], this->pOrgsAftTimestamps[level][partition], Tf, processedIds, valid, fossils);
        for (size_t partition = 0; partition < this->pRepsInIds[level].size(); ++partition) 
            processPartition(this->pRepsInIds[level][partition], this->pRepsInTimestamps[level][partition], Tf, processedIds, valid, fossils);
        for (size_t partition = 0; partition < this->pRepsAftIds[level].size(); ++partition) 
            processPartition(this->pRepsAftIds[level][partition], this->pRepsAftTimestamps[level][partition], Tf, processedIds, valid, fossils);
    }

    // Rebuild the index with valid records
    if (!fossils.empty()) {
        HINT_Reconstructable newIndex(this->leafPartitionExtent);
        for (const auto& record : valid)
            newIndex.insert(record);
        *this = move(newIndex);
    }

    return fossils;
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

