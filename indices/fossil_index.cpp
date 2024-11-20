#include "fossil_index.h"
#include <spatialindex/SpatialIndex.h>
#include <stdexcept>

using namespace std;
using namespace SpatialIndex;

FossilIndex::FossilIndex(string storageFile) {
    uint32_t pageSize = 4096;
    storageManager = StorageManager::createNewDiskStorageManager(storageFile, pageSize);
    if (!storageManager) 
        throw runtime_error("Failed to create disk storage manager.");

    long indexIdentifier = 1; // Use a valid index identifier
    rtree = RTree::createNewRTree(*storageManager, 0.7, 100, 100, 2, RTree::RV_RSTAR, indexIdentifier);
    if (!rtree) 
        throw runtime_error("Failed to create R-tree.");
}
FossilIndex::~FossilIndex() {}

void FossilIndex::insertInterval(int id, double start, double end) {
    double lowBounds[2] = {start, 0.0};
    double highBounds[2] = {end, 0.0};
    Region region(lowBounds, highBounds, 2);

    rtree->insertData(0, nullptr, region, id);
    ++objectCount;
}

int FossilIndex::query(double queryStart, double queryEnd) const {
    double queryLow[2] = {queryStart, 0.0};
    double queryHigh[2] = {queryEnd, 0.0};
    Region queryRegion(queryLow, queryHigh, 2);

    int count = 0;

    struct QueryVisitor : public IVisitor {
        int& countRef;
        QueryVisitor(int& count) : countRef(count) {}
        void visitNode(const INode&) override {}
        void visitData(const IData&) override { ++countRef; }
        void visitData(std::vector<const IData*>&) override {}
    } visitor(count);

    rtree->intersectsWithQuery(queryRegion, visitor);
    return count;
}

size_t FossilIndex::getObjectCount() const {
    return objectCount;
}

double getFileSizeInMB(const string& filePath) {
    ifstream file(filePath, ios::binary | ios::ate);
    if (!file) throw runtime_error("Could not open file: " + filePath);

    streamsize fileSize = file.tellg();
    file.close();
    return static_cast<double>(fileSize);
}

double FossilIndex::getDiskUsage() {  
    return getFileSizeInMB("fossil_index.db.dat") + getFileSizeInMB("fossil_index.db.idx");
}