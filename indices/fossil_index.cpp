#include "fossil_index.h"

using namespace std;

FossilIndex::FossilIndex() : rtree() {}

void FossilIndex::insertInterval(int id, double start, double end) {
    Box box(Point(start, start), Point(end, end));
    rtree.insert(make_pair(box, id));
}

int FossilIndex::query(double queryStart, double queryEnd) const {
    Box queryBox(Point(queryStart, queryStart), Point(queryEnd, queryEnd));
    vector<Value> results;
    rtree.query(bgi::intersects(queryBox), back_inserter(results));
    return results.size();
}

size_t FossilIndex::getObjectCount() const {
    return rtree.size();
}
