#ifndef FOSSIL_INDEX_H
#define FOSSIL_INDEX_H

#include <spatialindex/SpatialIndex.h>
#include <memory>
#include <vector>
#include <string>

using namespace SpatialIndex;

class FossilIndex {
public:
    FossilIndex(std::string storageFile);
    ~FossilIndex();

    void insertInterval(int id, double start, double end);
    int query(double queryStart, double queryEnd) const;
    size_t getObjectCount() const;
    double getDiskUsage();
    void getStatistics() const; 

private:
    IStorageManager* storageManager;
    ISpatialIndex* rtree;
    mutable size_t objectCount = 0;
};

#endif // FOSSIL_INDEX_H
