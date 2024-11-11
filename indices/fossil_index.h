// TODO: For now the fossil index in RTree. 
// Test other index structures.

#ifndef FOSSIL_INDEX_H
#define FOSSIL_INDEX_H

#include <memory>
#include <vector>
#include <boost/geometry.hpp>
#include <boost/geometry/index/rtree.hpp>

using namespace std;

namespace bg = boost::geometry;
namespace bgi = boost::geometry::index;

class FossilIndex {
public:
    using Point = bg::model::point<double, 2, bg::cs::cartesian>;
    using Box = bg::model::box<Point>;
    using Value = pair<Box, int>; // Box with interval ID or data reference

    FossilIndex();
    void insertInterval(int id, double start, double end);
    vector<int> query(double queryStart, double queryEnd) const;
    size_t getObjectCount() const;

private:
    bgi::rtree<Value, bgi::rstar<16>> rtree;
};

#endif // FOSSIL_INDEX_H
